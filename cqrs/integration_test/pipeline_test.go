//go:build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eventsalsa.dev/cqrs"
	"eventsalsa.dev/cqrs/command"
	"eventsalsa.dev/cqrs/query"
)

type fakeTxKey struct{}

var (
	errTxAlreadyActive   = errors.New("tx: transaction already active in context")
	errMissingTx         = errors.New("tx: no active transaction in context")
	errTransientConflict = errors.New("db: optimistic concurrency conflict")
	errDBUnavailable     = errors.New("db: connection pool exhausted")
	errInsufficientFunds = errors.New("domain: insufficient credit limit")
	errOrderNotFound     = errors.New("read: order not found")
)

type fakeTx struct {
	id      int64
	pending map[string]orderRecord
}

type orderRecord struct {
	OrderID     string
	CustomerID  string
	Status      string
	TraceID     string
	CommandID   string
	CausationID string
	Amount      int64
}

type memStore struct {
	mu          sync.Mutex
	committed   map[string]orderRecord
	nextTxID    atomic.Int64
	started     atomic.Int64
	committedN  atomic.Int64
	rolledBackN atomic.Int64
	dbDown      atomic.Bool
}

func newMemStore() *memStore {
	return &memStore{
		committed: make(map[string]orderRecord),
	}
}

// nonReentrantTxMiddleware mirrors deterministic UoW scoping: refuses nested starts
// with errTxAlreadyActive instead of silently masking wiring mistakes.
func (s *memStore) txMiddleware() command.Middleware {
	return func(next command.UntypedFunc) command.UntypedFunc {
		return func(ctx context.Context, cmd any) error {
			if _, active := ctx.Value(fakeTxKey{}).(*fakeTx); active {
				return errTxAlreadyActive
			}
			if s.dbDown.Load() {
				return errDBUnavailable
			}

			s.started.Add(1)
			tx := &fakeTx{
				id:      s.nextTxID.Add(1),
				pending: make(map[string]orderRecord),
			}
			txCtx := context.WithValue(ctx, fakeTxKey{}, tx)
			if err := next(txCtx, cmd); err != nil {
				s.rolledBackN.Add(1)
				return err
			}

			s.mu.Lock()
			for k, v := range tx.pending {
				s.committed[k] = v
			}
			s.mu.Unlock()
			s.committedN.Add(1)
			return nil
		}
	}
}

func (s *memStore) upsertInTx(ctx context.Context, rec *orderRecord) error {
	tx, ok := ctx.Value(fakeTxKey{}).(*fakeTx)
	if !ok || tx == nil {
		return errMissingTx
	}
	tx.pending[rec.OrderID] = *rec
	return nil
}

func (s *memStore) readCommitted(orderID string) (orderRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.committed[orderID]
	if !ok {
		return orderRecord{}, errOrderNotFound
	}
	return rec, nil
}

type placeOrder struct {
	OrderID    string
	CustomerID string
	Amount     int64
}

type confirmOrder struct {
	OrderID string
}

type getOrder struct {
	OrderID string
}

func TestIntegration_EndToEndWriteNestedPolicyAndQueryReadback(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	seq := atomic.Int64{}
	nextID := func() string {
		return fmt.Sprintf("uuid-%d", seq.Add(1))
	}

	var mu sync.Mutex
	var obsLog []cqrs.Observability

	cmdBus, err := command.NewBus(
		command.WithMiddleware(
			command.ObservabilityMiddleware(command.WithIDGenerator(nextID)),
			store.txMiddleware(),
		),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	command.MustRegister(cmdBus, command.Func[confirmOrder](func(ctx context.Context, cmd confirmOrder) error {
		obs, _ := cqrs.ObservabilityFromContext(ctx)
		mu.Lock()
		obsLog = append(obsLog, obs)
		mu.Unlock()

		existing, readErr := store.readCommitted(cmd.OrderID)
		if readErr != nil {
			return readErr
		}
		existing.Status = "CONFIRMED"
		existing.CommandID = obs.CommandID
		existing.CausationID = obs.CausationID
		return store.upsertInTx(ctx, &existing)
	}))

	command.MustRegister(cmdBus, command.Func[placeOrder](func(ctx context.Context, cmd placeOrder) error {
		obs, _ := cqrs.ObservabilityFromContext(ctx)
		mu.Lock()
		obsLog = append(obsLog, obs)
		mu.Unlock()

		return store.upsertInTx(ctx, &orderRecord{
			OrderID:    cmd.OrderID,
			CustomerID: cmd.CustomerID,
			Status:     "PLACED",
			TraceID:    obs.TraceID,
			CommandID:  obs.CommandID,
			Amount:     cmd.Amount,
		})
	}))

	queryBus, err := query.NewBus(
		query.WithMiddleware(
			query.ObservabilityMiddleware(),
			func(next query.UntypedFunc) query.UntypedFunc {
				return func(ctx context.Context, q any) (any, error) {
					readCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					return next(readCtx, q)
				}
			},
		),
	)
	if err != nil {
		t.Fatalf("query.NewBus: %v", err)
	}

	query.MustRegister(queryBus, query.Func[getOrder, orderRecord](func(_ context.Context, q getOrder) (orderRecord, error) {
		return store.readCommitted(q.OrderID)
	}))

	rootCtx := cqrs.WithTraceID(context.Background(), "http-trace-1")
	if dErr := cmdBus.Dispatch(rootCtx, placeOrder{OrderID: "ord-1", CustomerID: "c-1", Amount: 5000}); dErr != nil {
		t.Fatalf("Dispatch placeOrder: %v", dErr)
	}

	// Simulate event/policy dispatching follow-up ConfirmOrder carrying parent PlaceOrder observability
	placeObs := obsLog[0]
	policyCtx := cqrs.WithObservability(context.Background(), placeObs)
	if dErr := cmdBus.Dispatch(policyCtx, confirmOrder{OrderID: "ord-1"}); dErr != nil {
		t.Fatalf("Dispatch confirmOrder: %v", dErr)
	}

	view, qErr := query.Execute[orderRecord](rootCtx, queryBus, getOrder{OrderID: "ord-1"})
	if qErr != nil {
		t.Fatalf("Execute getOrder: %v", qErr)
	}
	if view.Status != "CONFIRMED" || view.TraceID != "http-trace-1" {
		t.Fatalf("unexpected view: %+v", view)
	}
	if view.CausationID != placeObs.CommandID || view.CommandID == "" || view.CommandID == placeObs.CommandID {
		t.Fatalf("broken causation link: place=%+v view=%+v", placeObs, view)
	}
}

func TestIntegration_ObservabilityRetryTxRollbackAndCommitComposition(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	type retryOrderCmd struct{ OrderID string }

	var attempt int
	var seenCmdIDs []string

	// Correct composition order: Observability (once) -> Retry -> TxMiddleware -> Handler.
	h, err := command.Wrap(
		command.Func[retryOrderCmd](func(ctx context.Context, cmd retryOrderCmd) error {
			attempt++
			seenCmdIDs = append(seenCmdIDs, cqrs.CommandIDFromContext(ctx))

			if wErr := store.upsertInTx(ctx, &orderRecord{
				OrderID: cmd.OrderID,
				Status:  fmt.Sprintf("ATTEMPT_%d", attempt),
			}); wErr != nil {
				return wErr
			}
			if attempt < 3 {
				return errTransientConflict
			}
			return nil
		}),
		command.ObservabilityMiddleware(),
		command.RetryMiddleware(command.RetryConfig{
			MaxAttempts: 3,
			BaseBackoff: time.Millisecond,
			MaxBackoff:  3 * time.Millisecond,
			RetryIf:     func(e error) bool { return errors.Is(e, errTransientConflict) },
		}),
		store.txMiddleware(),
	)
	if err != nil {
		t.Fatalf("command.Wrap: %v", err)
	}

	if runErr := h.Handle(context.Background(), retryOrderCmd{OrderID: "ord-retry"}); runErr != nil {
		t.Fatalf("Handle failed: %v", runErr)
	}
	if store.started.Load() != 3 || store.rolledBackN.Load() != 2 || store.committedN.Load() != 1 {
		t.Fatalf("unexpected tx counts: started=%d rolledBack=%d committed=%d",
			store.started.Load(), store.rolledBackN.Load(), store.committedN.Load())
	}
	if seenCmdIDs[0] != seenCmdIDs[1] || seenCmdIDs[1] != seenCmdIDs[2] || seenCmdIDs[0] == "" {
		t.Fatalf("CommandID must stay constant across tx retries, got %v", seenCmdIDs)
	}
	got, readErr := store.readCommitted("ord-retry")
	if readErr != nil || got.Status != "ATTEMPT_3" {
		t.Fatalf("expected committed ATTEMPT_3 without dirty earlier writes, got (%+v, %v)", got, readErr)
	}
}

func TestIntegration_DoubleTxFailsDeterministicallyAndOpacityDistinguishesInfra(t *testing.T) {
	t.Parallel()

	store := newMemStore()

	type nestedBugCmd struct{ OrderID string }
	type chargeCmd struct{ Amount int64 }

	var rejectedCount atomic.Int64
	edgeBus, err := command.NewBus(
		command.WithMiddleware(store.txMiddleware()),
		command.WithErrorClassifier(func(e error) error {
			if errors.Is(e, errInsufficientFunds) {
				return command.Reject(e)
			}
			return e
		}),
		command.WithOnRejected(func(context.Context, any, error) {
			rejectedCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("NewBus edge: %v", err)
	}

	// Route mistakenly installs a second TxMiddleware inside outer bus TxMiddleware.
	command.MustRegister(edgeBus, command.Func[nestedBugCmd](func(ctx context.Context, c nestedBugCmd) error {
		return store.upsertInTx(ctx, &orderRecord{OrderID: c.OrderID, Status: "SHOULD_NOT_COMMIT"})
	}), store.txMiddleware())

	command.MustRegister(edgeBus, command.Func[chargeCmd](func(ctx context.Context, c chargeCmd) error {
		if wErr := store.upsertInTx(ctx, &orderRecord{OrderID: "partial", Amount: c.Amount}); wErr != nil {
			return wErr
		}
		return errInsufficientFunds
	}))

	// 1. Nested TxMiddleware fails loudly with errTxAlreadyActive & rolls back outer tx.
	errDouble := edgeBus.Dispatch(context.Background(), nestedBugCmd{OrderID: "double"})
	if !errors.Is(errDouble, errTxAlreadyActive) {
		t.Fatalf("expected errTxAlreadyActive, got %v", errDouble)
	}
	if store.committedN.Load() != 0 || store.rolledBackN.Load() != 1 {
		t.Fatalf("expected outer tx rollback on nested tx bug, got committed=%d rolledBack=%d",
			store.committedN.Load(), store.rolledBackN.Load())
	}

	// 2. Domain rejection rolls back tx AND masks rejection as nil at edge boundary.
	if dErr := edgeBus.Dispatch(context.Background(), chargeCmd{Amount: 999}); dErr != nil {
		t.Fatalf("expected domain rejection to be masked at edge bus, got %v", dErr)
	}
	if rejectedCount.Load() != 1 {
		t.Fatalf("expected rejectedCount=1, got %d", rejectedCount.Load())
	}
	if _, readErr := store.readCommitted("partial"); !errors.Is(readErr, errOrderNotFound) {
		t.Fatalf("partial write should have rolled back on rejection, got %v", readErr)
	}

	// 3. Simulated DB outage must NOT be masked by default rejection-masking policy!
	store.dbDown.Store(true)
	if outErr := edgeBus.Dispatch(context.Background(), chargeCmd{Amount: 1}); !errors.Is(outErr, errDBUnavailable) {
		t.Fatalf("DB outage must surface untouched, got %v", outErr)
	}
	if rejectedCount.Load() != 1 {
		t.Fatalf("DB outage must not trigger onRejected, count=%d", rejectedCount.Load())
	}

	// 4. Passthrough bus returns raw domain rejection for inbound async consumers.
	store.dbDown.Store(false)
	internalBus, err := command.NewBus(
		command.WithMiddleware(store.txMiddleware()),
		command.WithPassthroughErrors(),
	)
	if err != nil {
		t.Fatalf("NewBus internal: %v", err)
	}
	command.MustRegister(internalBus, command.Func[chargeCmd](func(context.Context, chargeCmd) error {
		return errInsufficientFunds
	}))
	if passthroughErr := internalBus.Dispatch(context.Background(), chargeCmd{Amount: 50}); !errors.Is(passthroughErr, errInsufficientFunds) {
		t.Fatalf("expected raw errInsufficientFunds, got %v", passthroughErr)
	}
}

func TestIntegration_StrictOpaquePanicRecoveryAndConcurrentStress(t *testing.T) {
	t.Parallel()

	type parentWork struct{ Worker int }
	type childWork struct{ Worker int }
	type readWork struct{ Worker int }
	type panicCmd struct{}

	var (
		mu       sync.Mutex
		sinkErrs []error
		state    = make(map[int]int)
	)

	recoverMW := func(next command.UntypedFunc) command.UntypedFunc {
		return func(ctx context.Context, cmd any) (retErr error) {
			defer func() {
				if r := recover(); r != nil {
					retErr = fmt.Errorf("panic recovered: %v", r)
				}
			}()
			return next(ctx, cmd)
		}
	}

	bus, err := command.NewBus(
		command.WithMiddleware(
			recoverMW,
			command.ObservabilityMiddleware(),
		),
		command.WithStrictOpaqueDispatch(func(_ context.Context, _ any, e error) {
			mu.Lock()
			sinkErrs = append(sinkErrs, e)
			mu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("NewBus: %v", err)
	}

	command.MustRegister(bus, command.Func[panicCmd](func(context.Context, panicCmd) error {
		panic("unexpected nil deref")
	}))
	command.MustRegister(bus, command.Func[childWork](func(ctx context.Context, c childWork) error {
		if cqrs.CausationIDFromContext(ctx) == "" {
			return errors.New("missing parent causation ID")
		}
		mu.Lock()
		state[c.Worker]++
		mu.Unlock()
		return nil
	}))
	command.MustRegister(bus, command.Func[parentWork](func(ctx context.Context, c parentWork) error {
		return bus.Dispatch(ctx, childWork(c))
	}))

	qBus, err := query.NewBus()
	if err != nil {
		t.Fatalf("query.NewBus: %v", err)
	}
	query.MustRegister(qBus, query.Func[readWork, int](func(_ context.Context, q readWork) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return state[q.Worker], nil
	}))

	// Recovered panic flows safely to sink under StrictOpaque without crashing caller.
	if dErr := bus.Dispatch(context.Background(), panicCmd{}); dErr != nil {
		t.Fatalf("StrictOpaque with recovery should return nil, got %v", dErr)
	}
	mu.Lock()
	if len(sinkErrs) != 1 {
		mu.Unlock()
		t.Fatalf("expected 1 recovered panic in sink, got %d", len(sinkErrs))
	}
	mu.Unlock()

	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers*2)
	for i := range workers {
		wg.Go(func() {
			ctx := cqrs.WithTraceID(context.Background(), fmt.Sprintf("trace-%d", i))
			if dErr := bus.Dispatch(ctx, parentWork{Worker: i}); dErr != nil {
				errs <- dErr
				return
			}
			val, qErr := query.Execute[int](ctx, qBus, readWork{Worker: i})
			if qErr != nil || val != 1 {
				errs <- fmt.Errorf("worker %d got (%d, %v), want (1, nil)", i, val, qErr)
			}
		})
	}
	wg.Wait()
	close(errs)

	for e := range errs {
		t.Fatal(e)
	}
}
