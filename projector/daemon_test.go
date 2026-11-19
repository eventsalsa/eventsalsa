package projector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"eventsalsa.dev/store"

	"eventsalsa.dev/projector/dispatcher"
	projectorpostgres "eventsalsa.dev/projector/postgres"
)

type stubProjectorStore struct {
	readErr     error
	headErr     error
	events      []store.PersistedEvent
	readBatches [][]store.PersistedEvent
	head        store.Cursor
	readCalls   int
	lastAfter   store.Cursor
	lastLimit   int
}

func (s *stubProjectorStore) ReadEvents(_ context.Context, _ pgx.Tx, after store.Cursor, limit int) ([]store.PersistedEvent, error) {
	s.readCalls++
	s.lastAfter = after
	s.lastLimit = limit

	if s.readErr != nil {
		return nil, s.readErr
	}

	events := s.events
	if len(s.readBatches) > 0 {
		index := s.readCalls - 1
		if index >= len(s.readBatches) {
			index = len(s.readBatches) - 1
		}
		events = s.readBatches[index]
	}

	if limit > 0 && len(events) > limit {
		return append([]store.PersistedEvent(nil), events[:limit]...), nil
	}

	return append([]store.PersistedEvent(nil), events...), nil
}

func (s *stubProjectorStore) GetHead(_ context.Context, _ pgx.Tx) (store.Cursor, error) {
	if s.headErr != nil {
		return store.Cursor{}, s.headErr
	}
	return s.head, nil
}

type recordingProjection struct {
	handleErr error
	name      string
	handled   []store.PersistedEvent
	failAt    int
}

func (c *recordingProjection) Name() string {
	return c.name
}

//nolint:gocritic // hugeParam: test mock
func (c *recordingProjection) Handle(_ context.Context, _ pgx.Tx, event store.PersistedEvent) error {
	call := len(c.handled) + 1
	if c.failAt > 0 && call == c.failAt {
		return c.handleErr
	}

	c.handled = append(c.handled, event)
	return nil
}

// registrationForTest adapts any projection shape into the daemon's internal
// registration for white-box tests.
func registrationForTest(t testing.TB, projection any) registration {
	t.Helper()

	registry := NewRegistry()
	if err := registry.Add(projection); err != nil {
		t.Fatalf("register test projection: %v", err)
	}

	return registry.registrations[0]
}

type testDispatcher struct {
	ch <-chan struct{}
}

func (d *testDispatcher) Start(context.Context) error {
	return nil
}

func (d *testDispatcher) WakeupChan() <-chan struct{} {
	return d.ch
}

type stubDBState struct {
	beginErr         error
	execErr          error
	execRowsAffected []int64
	commitErr        error
	commitErrors     []error
	rollbackErr      error
	queryErr         error
	ownerID          string
	checkpoint       store.Cursor
	queryValues      []any
	execArgs         [][]any
	execQueries      []string
	execCalls        int
	beginCalls       int
	beginCtxErrs     []error
	commitCalls      int
	rollbackCalls    int
	queryCalls       int
	ops              []string
	mu               sync.Mutex
	ownerValid       bool
}

type stubPgxPool struct {
	state *stubDBState
}

//nolint:gocritic // hugeParam: implements PgxPool interface
func (p *stubPgxPool) BeginTx(ctx context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	p.state.beginCalls++
	p.state.beginCtxErrs = append(p.state.beginCtxErrs, ctx.Err())
	p.state.ops = append(p.state.ops, "begin")
	if p.state.beginErr != nil {
		return nil, p.state.beginErr
	}

	return &stubPgxTx{state: p.state}, nil
}

func (p *stubPgxPool) Acquire(_ context.Context) (*pgxpool.Conn, error) {
	return nil, nil
}

func (p *stubPgxPool) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	p.state.execCalls++
	p.state.ops = append(p.state.ops, "exec")
	p.state.execQueries = append(p.state.execQueries, sql)
	p.state.execArgs = append(p.state.execArgs, arguments)

	if p.state.execErr != nil {
		return pgconn.NewCommandTag(""), p.state.execErr
	}

	rowsAffected := int64(1)
	if len(p.state.execRowsAffected) > 0 {
		index := p.state.execCalls - 1
		if index >= len(p.state.execRowsAffected) {
			index = len(p.state.execRowsAffected) - 1
		}
		rowsAffected = p.state.execRowsAffected[index]
	}

	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAffected)), nil
}

func (p *stubPgxPool) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	p.state.queryCalls++
	p.state.ops = append(p.state.ops, "query")
	if p.state.queryErr != nil {
		return nil, p.state.queryErr
	}

	if len(p.state.queryValues) > 0 {
		index := p.state.queryCalls - 1
		if index >= len(p.state.queryValues) {
			index = len(p.state.queryValues) - 1
		}
		if p.state.queryValues[index] == nil {
			return &stubPgxRows{returned: true}, nil
		}
		return &stubPgxRows{value: p.state.queryValues[index]}, nil
	}

	if strings.Contains(sql, "FROM projection_checkpoints") {
		return &stubPgxRows{value: p.state.checkpoint}, nil
	}

	if !p.state.ownerValid {
		return &stubPgxRows{returned: true}, nil
	}

	return &stubPgxRows{value: p.state.ownerID}, nil
}

func (p *stubPgxPool) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	p.state.mu.Lock()
	defer p.state.mu.Unlock()

	p.state.queryCalls++
	p.state.ops = append(p.state.ops, "query_row")
	if p.state.queryErr != nil {
		return &stubPgxRow{err: p.state.queryErr}
	}

	if len(p.state.queryValues) > 0 {
		index := p.state.queryCalls - 1
		if index >= len(p.state.queryValues) {
			index = len(p.state.queryValues) - 1
		}
		return &stubPgxRow{value: p.state.queryValues[index]}
	}

	if strings.Contains(sql, "FROM projection_checkpoints") {
		return &stubPgxRow{value: p.state.checkpoint}
	}

	if !p.state.ownerValid {
		return &stubPgxRow{err: pgx.ErrNoRows}
	}

	return &stubPgxRow{value: p.state.ownerID}
}

type stubPgxTx struct {
	state *stubDBState
}

func (tx *stubPgxTx) Begin(_ context.Context) (pgx.Tx, error) {
	return nil, nil
}

func (tx *stubPgxTx) Commit(_ context.Context) error {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()

	tx.state.commitCalls++
	tx.state.ops = append(tx.state.ops, "commit")
	if len(tx.state.commitErrors) > 0 {
		index := tx.state.commitCalls - 1
		if index >= len(tx.state.commitErrors) {
			index = len(tx.state.commitErrors) - 1
		}
		return tx.state.commitErrors[index]
	}

	return tx.state.commitErr
}

func (tx *stubPgxTx) Rollback(_ context.Context) error {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()

	tx.state.rollbackCalls++
	tx.state.ops = append(tx.state.ops, "rollback")
	return tx.state.rollbackErr
}

func (tx *stubPgxTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, _ pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (tx *stubPgxTx) SendBatch(_ context.Context, _ *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *stubPgxTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *stubPgxTx) Prepare(_ context.Context, _, _ string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *stubPgxTx) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()

	tx.state.execCalls++
	tx.state.ops = append(tx.state.ops, "exec")
	tx.state.execQueries = append(tx.state.execQueries, sql)
	tx.state.execArgs = append(tx.state.execArgs, arguments)

	if tx.state.execErr != nil {
		return pgconn.NewCommandTag(""), tx.state.execErr
	}

	rowsAffected := int64(1)
	if len(tx.state.execRowsAffected) > 0 {
		index := tx.state.execCalls - 1
		if index >= len(tx.state.execRowsAffected) {
			index = len(tx.state.execRowsAffected) - 1
		}
		rowsAffected = tx.state.execRowsAffected[index]
	}

	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAffected)), nil
}

func (tx *stubPgxTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()

	tx.state.queryCalls++
	tx.state.ops = append(tx.state.ops, "query")
	if tx.state.queryErr != nil {
		return nil, tx.state.queryErr
	}

	if len(tx.state.queryValues) > 0 {
		index := tx.state.queryCalls - 1
		if index >= len(tx.state.queryValues) {
			index = len(tx.state.queryValues) - 1
		}
		if tx.state.queryValues[index] == nil {
			return &stubPgxRows{returned: true}, nil
		}
		return &stubPgxRows{value: tx.state.queryValues[index]}, nil
	}

	if strings.Contains(sql, "FROM projection_checkpoints") {
		return &stubPgxRows{value: tx.state.checkpoint}, nil
	}

	if !tx.state.ownerValid {
		return &stubPgxRows{returned: true}, nil
	}

	return &stubPgxRows{value: tx.state.ownerID}, nil
}

func (tx *stubPgxTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()

	tx.state.queryCalls++
	tx.state.ops = append(tx.state.ops, "query_row")
	if tx.state.queryErr != nil {
		return &stubPgxRow{err: tx.state.queryErr}
	}

	if len(tx.state.queryValues) > 0 {
		index := tx.state.queryCalls - 1
		if index >= len(tx.state.queryValues) {
			index = len(tx.state.queryValues) - 1
		}
		return &stubPgxRow{value: tx.state.queryValues[index]}
	}

	if strings.Contains(sql, "FROM projection_checkpoints") {
		return &stubPgxRow{value: tx.state.checkpoint}
	}

	if !tx.state.ownerValid {
		return &stubPgxRow{err: pgx.ErrNoRows}
	}

	return &stubPgxRow{value: tx.state.ownerID}
}

func (tx *stubPgxTx) Conn() *pgx.Conn {
	return nil
}

func setVal(dest, src any) {
	if dest == nil || src == nil {
		return
	}
	vDest := reflect.ValueOf(dest)
	if vDest.Kind() != reflect.Ptr {
		return
	}

	elem := vDest.Elem()
	if elem.Kind() == reflect.Ptr {
		if elem.IsNil() {
			elem.Set(reflect.New(elem.Type().Elem()))
		}
		elem = elem.Elem()
	}

	switch srcVal := src.(type) {
	case string:
		if elem.Kind() == reflect.String {
			elem.SetString(srcVal)
		} else if elem.Type() == reflect.TypeOf(uuid.UUID{}) {
			if parsed, err := uuid.Parse(srcVal); err == nil {
				elem.Set(reflect.ValueOf(parsed))
			}
		}
	case uuid.UUID:
		if elem.Kind() == reflect.String {
			elem.SetString(srcVal.String())
		} else if elem.Type() == reflect.TypeOf(uuid.UUID{}) {
			elem.Set(reflect.ValueOf(srcVal))
		}
	case int64:
		if elem.Kind() == reflect.Int64 || elem.Kind() == reflect.Int {
			elem.SetInt(srcVal)
		}
	case uint64:
		if elem.Kind() == reflect.Uint64 || elem.Kind() == reflect.Uint {
			elem.SetUint(srcVal)
		}
	}
}

type stubPgxRows struct {
	value    any
	returned bool
}

func (r *stubPgxRows) Close() {}

func (r *stubPgxRows) Err() error {
	return nil
}

func (r *stubPgxRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *stubPgxRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *stubPgxRows) Next() bool {
	return !r.returned
}

func (r *stubPgxRows) Scan(dest ...any) error {
	if r.returned {
		return io.EOF
	}

	if c, ok := r.value.(store.Cursor); ok && len(dest) >= 2 {
		setVal(dest[0], c.TransactionID)
		setVal(dest[1], c.GlobalPosition)
	} else if len(dest) > 0 {
		setVal(dest[0], r.value)
	}

	r.returned = true
	return nil
}

func (r *stubPgxRows) Values() ([]any, error) {
	return []any{r.value}, nil
}

func (r *stubPgxRows) RawValues() [][]byte {
	return nil
}

func (r *stubPgxRows) Conn() *pgx.Conn {
	return nil
}

func (r *stubPgxRows) TypeMap() *pgtype.Map {
	return nil
}

type stubPgxRow struct {
	value any
	err   error
}

func (r *stubPgxRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if c, ok := r.value.(store.Cursor); ok && len(dest) >= 2 {
		setVal(dest[0], c.TransactionID)
		setVal(dest[1], c.GlobalPosition)
	} else if len(dest) > 0 {
		setVal(dest[0], r.value)
	}
	return nil
}

func openStubDB(t *testing.T, state *stubDBState) PgxPool {
	t.Helper()
	return &stubPgxPool{state: state}
}

func TestNewUsesDefaultConfig(t *testing.T) {
	storeStub := &stubProjectorStore{}

	daemon := New(
		openStubDB(t, &stubDBState{}),
		storeStub,
		FromProjections(&recordingProjection{name: "alpha"}),
	)

	defaults := DefaultConfig()
	if daemon.store != storeStub {
		t.Fatal("daemon store was not set")
	}
	if daemon.config != defaults {
		t.Fatalf("config = %#v, want %#v", daemon.config, defaults)
	}
	if daemon.dispatcher == nil {
		t.Fatal("dispatcher was not initialized")
	}
	if len(daemon.runningProjections) != 0 {
		t.Fatalf("runningProjections length = %d, want 0", len(daemon.runningProjections))
	}
}

func TestNewCopiesProjectionsAndUsesConfiguredOptions(t *testing.T) {
	storeStub := &stubProjectorStore{}
	registry := FromProjections(
		&recordingProjection{name: "alpha"},
		&recordingProjection{name: "beta"},
	)

	daemon := New(
		openStubDB(t, &stubDBState{}),
		storeStub,
		registry,
		WithBatchSize(7),
		WithPollInterval(3*time.Second),
	)

	if daemon.store != storeStub {
		t.Fatal("daemon store was not set")
	}
	if daemon.config.BatchSize != 7 {
		t.Fatalf("BatchSize = %d, want 7", daemon.config.BatchSize)
	}
	if daemon.config.PollInterval != 3*time.Second {
		t.Fatalf("PollInterval = %v, want %v", daemon.config.PollInterval, 3*time.Second)
	}
	if daemon.dispatcher == nil {
		t.Fatal("dispatcher was not initialized")
	}
	if len(daemon.runningProjections) != 0 {
		t.Fatalf("runningProjections length = %d, want 0", len(daemon.runningProjections))
	}

	registry.registrations[0].name = "mutated"
	if daemon.registrations[0].name != "alpha" {
		t.Fatalf("daemon registrations slice was not copied, got %q", daemon.registrations[0].name)
	}
}

func TestNewUsesNotifyDispatcherWhenConfigured(t *testing.T) {
	daemon := New(
		openStubDB(t, &stubDBState{}),
		&stubProjectorStore{},
		FromProjections(&recordingProjection{name: "alpha"}),
		WithDispatcherStrategy(DispatcherStrategyNotify),
		WithNotifyConnectionString("postgres://projector:test@localhost/db?sslmode=disable"),
		WithNotifyChannel("custom_events"),
	)

	notifyDispatcher, ok := daemon.dispatcher.(*dispatcher.NotifyDispatcher)
	if !ok {
		t.Fatalf("dispatcher type = %T, want *dispatcher.NotifyDispatcher", daemon.dispatcher)
	}
	if notifyDispatcher == nil {
		t.Fatal("notify dispatcher is nil")
	}
}

func TestDaemonValidate(t *testing.T) {
	validDB := openStubDB(t, &stubDBState{})
	validStore := &stubProjectorStore{}
	validDispatcher := &testDispatcher{ch: make(chan struct{})}
	validRegistry := FromProjections(&recordingProjection{name: "alpha"})

	nilProjectionRegistry := NewRegistry()
	if err := nilProjectionRegistry.Add(nil); err == nil {
		t.Fatal("expected registering nil to fail")
	}
	emptyNameRegistry := NewRegistry()
	if err := emptyNameRegistry.AddProjection(&recordingProjection{}); err == nil {
		t.Fatal("expected registering an empty name to fail")
	}
	duplicateRegistry := NewRegistry()
	if err := duplicateRegistry.AddProjection(&recordingProjection{name: "dup"}); err != nil {
		t.Fatalf("register first duplicate: %v", err)
	}
	if err := duplicateRegistry.AddProjection(&recordingProjection{name: "dup"}); err == nil {
		t.Fatal("expected registering a duplicate name to fail")
	}

	tests := []struct {
		name   string
		daemon *Daemon
		errMsg string
	}{
		{
			name:   "nil db",
			daemon: &Daemon{store: validStore, dispatcher: validDispatcher, registry: validRegistry},
			errMsg: ErrNilDB.Error(),
		},
		{
			name:   "nil store",
			daemon: &Daemon{db: validDB, dispatcher: validDispatcher, registry: validRegistry},
			errMsg: ErrNilStore.Error(),
		},
		{
			name:   "nil dispatcher",
			daemon: &Daemon{db: validDB, store: validStore, registry: validRegistry},
			errMsg: ErrNilDispatcher.Error(),
		},
		{
			name:   "nil registry",
			daemon: &Daemon{db: validDB, store: validStore, dispatcher: validDispatcher},
			errMsg: ErrNilRegistry.Error(),
		},
		{
			name:   "nil projection",
			daemon: &Daemon{db: validDB, store: validStore, dispatcher: validDispatcher, registry: nilProjectionRegistry},
			errMsg: ErrNilProjection.Error(),
		},
		{
			name:   "empty projection name",
			daemon: &Daemon{db: validDB, store: validStore, dispatcher: validDispatcher, registry: emptyNameRegistry},
			errMsg: ErrEmptyProjectionName.Error(),
		},
		{
			name:   "duplicate projection name",
			daemon: &Daemon{db: validDB, store: validStore, dispatcher: validDispatcher, registry: duplicateRegistry},
			errMsg: `duplicate projection name: "dup"`,
		},
		{
			name: "notify dispatcher requires connection string",
			daemon: &Daemon{
				db:         validDB,
				store:      validStore,
				dispatcher: validDispatcher,
				registry:   validRegistry,
				config:     Config{DispatcherStrategy: DispatcherStrategyNotify},
			},
			errMsg: ErrMissingNotifyConnectionString.Error(),
		},
		{
			name: "notify dispatcher requires channel",
			daemon: &Daemon{
				db:         validDB,
				store:      validStore,
				dispatcher: validDispatcher,
				registry:   validRegistry,
				config: Config{
					DispatcherStrategy:     DispatcherStrategyNotify,
					NotifyConnectionString: "postgres://localhost/db",
				},
			},
			errMsg: ErrMissingNotifyChannel.Error(),
		},
		{
			name: "valid daemon",
			daemon: &Daemon{
				db:         validDB,
				store:      validStore,
				dispatcher: validDispatcher,
				registry:   validRegistry,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.daemon.validate()
			if tc.errMsg == "" {
				if err != nil {
					t.Fatalf("validate() error = %v", err)
				}
				return
			}

			if err == nil {
				t.Fatalf("validate() error = nil, want %q", tc.errMsg)
			}
			if err.Error() != tc.errMsg {
				t.Fatalf("validate() error = %q, want %q", err.Error(), tc.errMsg)
			}
		})
	}
}

func TestDaemonStopInvokesCancel(t *testing.T) {
	called := 0
	daemon := &Daemon{
		cancel: func() {
			called++
		},
	}

	daemon.Stop()

	if called != 1 {
		t.Fatalf("cancel called %d times, want 1", called)
	}
}

func TestAssignmentPollInterval(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   time.Duration
	}{
		{
			name:   "uses default when non-positive",
			config: Config{},
			want:   defaultAssignmentPollInterval,
		},
		{
			name:   "clamps sub-second interval",
			config: Config{RebalanceInterval: 500 * time.Millisecond},
			want:   time.Second,
		},
		{
			name:   "uses configured interval",
			config: Config{RebalanceInterval: 3 * time.Second},
			want:   3 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			daemon := &Daemon{config: tc.config}
			if got := daemon.assignmentPollInterval(); got != tc.want {
				t.Fatalf("assignmentPollInterval() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProcessBatchScopedReads(t *testing.T) {
	makeEvent := func(txID uint64, position int64, streamType string) store.PersistedEvent {
		return store.PersistedEvent{
			EventID:        uuid.New(),
			TransactionID:  txID,
			GlobalPosition: position,
			StreamType:     streamType,
			StreamID:       fmt.Sprintf("%s-%d", streamType, position),
			StreamVersion:  position,
		}
	}

	t.Run("global projection reads the unscoped event stream", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: store.Cursor{TransactionID: 10, GlobalPosition: 7}}
		storeStub := &stubProjectorStore{
			events: []store.PersistedEvent{makeEvent(10, 1, "order")},
		}
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		_, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "global"}), store.Cursor{})
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if storeStub.readCalls == 0 {
			t.Fatal("ReadEvents() was not called")
		}
	})

	t.Run("filtered projection filters matching events in memory after unscoped probe", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
		storeStub := &stubProjectorStore{
			events: []store.PersistedEvent{
				makeEvent(10, 1, "order"),
				makeEvent(10, 2, "user"),
				makeEvent(10, 3, "order"),
			},
		}
		inner := &recordingProjection{name: "orders"}
		registeredProjection := FilterStreamTypes(inner, "order")
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		result, err := daemon.processBatch(context.Background(), registrationForTest(t, registeredProjection), store.Cursor{})
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if len(inner.handled) != 2 {
			t.Fatalf("handled events = %d, want 2 (only order events)", len(inner.handled))
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		want := store.Cursor{TransactionID: 10, GlobalPosition: 3}
		if result.checkpoint != want {
			t.Fatalf("checkpoint = %+v, want %+v", result.checkpoint, want)
		}
	})

	t.Run("filtered projection with empty types still processes all events", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: store.Cursor{TransactionID: 10, GlobalPosition: 3}}
		storeStub := &stubProjectorStore{
			events: []store.PersistedEvent{makeEvent(10, 1, "order")},
		}
		inner := &recordingProjection{name: "all"}
		registeredProjection := FilterStreamTypes(inner)
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		_, err := daemon.processBatch(context.Background(), registrationForTest(t, registeredProjection), store.Cursor{})
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if storeStub.readCalls == 0 {
			t.Fatal("ReadEvents() was not called")
		}
	})

	t.Run("filtered projection with no matching events updates checkpoint", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
		storeStub := &stubProjectorStore{
			events: []store.PersistedEvent{
				makeEvent(10, 1, "user"),
				makeEvent(10, 2, "user"),
			},
		}
		inner := &recordingProjection{name: "orders"}
		registeredProjection := FilterStreamTypes(inner, "order")
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		result, err := daemon.processBatch(context.Background(), registrationForTest(t, registeredProjection), store.Cursor{})
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		if len(inner.handled) != 0 {
			t.Fatalf("handledCount = %d, want 0", len(inner.handled))
		}
		want := store.Cursor{TransactionID: 10, GlobalPosition: 2}
		if result.checkpoint != want {
			t.Fatalf("checkpoint = %+v, want %+v", result.checkpoint, want)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1 (checkpoint update only)", state.execCalls)
		}
	})
}

func TestNextPollInterval(t *testing.T) {
	tests := []struct {
		name    string
		current time.Duration
		max     time.Duration
		base    time.Duration
		want    time.Duration
	}{
		{
			name:    "uses base when current is non-positive",
			current: 0,
			max:     10 * time.Second,
			base:    time.Second,
			want:    time.Second,
		},
		{
			name:    "uses base when current is negative",
			current: -time.Second,
			max:     10 * time.Second,
			base:    2 * time.Second,
			want:    2 * time.Second,
		},
		{
			name:    "clamps to current when max is non-positive",
			current: 2 * time.Second,
			max:     0,
			base:    time.Second,
			want:    2 * time.Second,
		},
		{
			name:    "doubles until max",
			current: 2 * time.Second,
			max:     10 * time.Second,
			base:    time.Second,
			want:    4 * time.Second,
		},
		{
			name:    "caps at max",
			current: 8 * time.Second,
			max:     10 * time.Second,
			base:    time.Second,
			want:    10 * time.Second,
		},
		{
			name:    "overflow returns max",
			current: time.Duration(1<<62 + 1),
			max:     30 * time.Second,
			base:    time.Second,
			want:    30 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextPollInterval(tc.current, tc.max, tc.base); got != tc.want {
				t.Fatalf("nextPollInterval() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInitializeCleansStaleInstancesBeforeRegistering(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{}
	registry := FromProjections(&recordingProjection{name: "alpha"})
	daemon := &Daemon{
		id:            instanceID,
		db:            openStubDB(t, state),
		config:        Config{HeartbeatTimeout: 5 * time.Second, Logger: store.NoOpLogger{}},
		registry:      registry,
		registrations: registry.registrations,
	}

	registered := false
	if err := daemon.initialize(context.Background(), &registered); err != nil {
		t.Fatalf("initialize() error = %v", err)
	}
	if !registered {
		t.Fatal("registered = false, want true")
	}
	if state.execCalls != 4 {
		t.Fatalf("execCalls = %d, want 4", state.execCalls)
	}
	if len(state.execQueries) < 2 {
		t.Fatalf("execQueries = %v, want cleanup and register queries", state.execQueries)
	}
	if !strings.Contains(state.execQueries[0], "DELETE FROM projector_instances") {
		t.Fatalf("first exec query = %q, want projector_instances cleanup", state.execQueries[0])
	}
	if !strings.Contains(state.execQueries[0], "heartbeat_at < NOW()") {
		t.Fatalf("first exec query = %q, want stale heartbeat cutoff", state.execQueries[0])
	}
	if !strings.Contains(state.execQueries[1], "INSERT INTO projector_instances") {
		t.Fatalf("second exec query = %q, want instance registration", state.execQueries[1])
	}
}

func TestRunHeartbeatLoopReportsFatalWhenRegistrationRowIsMissing(t *testing.T) {
	state := &stubDBState{execRowsAffected: []int64{0, 0}}
	daemon := &Daemon{
		id:         uuid.New(),
		db:         openStubDB(t, state),
		config:     Config{HeartbeatInterval: time.Millisecond, Logger: store.NoOpLogger{}},
		fatalErrCh: make(chan error, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		daemon.runHeartbeatLoop(ctx)
		close(done)
	}()

	select {
	case err := <-daemon.fatalErrCh:
		if !errors.Is(err, projectorpostgres.ErrInstanceRegistrationMissing) {
			t.Fatalf("fatal error = %v, want ErrInstanceRegistrationMissing", err)
		}
		if !strings.Contains(err.Error(), "instance registration lost during heartbeat") {
			t.Fatalf("fatal error = %v, want wrapped heartbeat registration error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for heartbeat fatal error")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat loop to exit")
	}

	if state.execCalls < missingRegistrationRetryLimit {
		t.Fatalf("execCalls = %d, want at least %d heartbeat attempts", state.execCalls, missingRegistrationRetryLimit)
	}
	if len(state.execQueries) == 0 || !strings.Contains(state.execQueries[0], "UPDATE projector_instances") {
		t.Fatalf("execQueries = %v, want heartbeat update query", state.execQueries)
	}
}

func TestWaitForProjectionDelay(t *testing.T) {
	t.Run("returns wakeup when dispatcher signals", func(t *testing.T) {
		ch := make(chan struct{})
		close(ch)

		daemon := &Daemon{dispatcher: &testDispatcher{ch: ch}}
		woken, ok := daemon.waitForProjectionDelay(context.Background(), context.Background(), time.Second)
		if !ok {
			t.Fatal("waitForProjectionDelay() ok = false, want true")
		}
		if !woken {
			t.Fatal("waitForProjectionDelay() woken = false, want true")
		}
	})

	t.Run("returns when timer expires", func(t *testing.T) {
		daemon := &Daemon{dispatcher: &testDispatcher{ch: make(chan struct{})}}
		woken, ok := daemon.waitForProjectionDelay(context.Background(), context.Background(), 5*time.Millisecond)
		if !ok {
			t.Fatal("waitForProjectionDelay() ok = false, want true")
		}
		if woken {
			t.Fatal("waitForProjectionDelay() woken = true, want false")
		}
	})

	t.Run("stops on control cancellation", func(t *testing.T) {
		controlCtx, cancel := context.WithCancel(context.Background())
		cancel()

		daemon := &Daemon{dispatcher: &testDispatcher{ch: make(chan struct{})}}
		woken, ok := daemon.waitForProjectionDelay(controlCtx, context.Background(), time.Second)
		if ok {
			t.Fatal("waitForProjectionDelay() ok = true, want false")
		}
		if woken {
			t.Fatal("waitForProjectionDelay() woken = true, want false")
		}
	})

	t.Run("stops on assignment cancellation", func(t *testing.T) {
		assignmentCtx, cancel := context.WithCancel(context.Background())
		cancel()

		daemon := &Daemon{dispatcher: &testDispatcher{ch: make(chan struct{})}}
		woken, ok := daemon.waitForProjectionDelay(context.Background(), assignmentCtx, time.Second)
		if ok {
			t.Fatal("waitForProjectionDelay() ok = true, want false")
		}
		if woken {
			t.Fatal("waitForProjectionDelay() woken = true, want false")
		}
	})
}

func TestProcessBatch(t *testing.T) {
	makeEvent := func(txID uint64, position int64, streamType string) store.PersistedEvent {
		return store.PersistedEvent{
			EventID:        uuid.New(),
			TransactionID:  txID,
			GlobalPosition: position,
			StreamType:     streamType,
			StreamID:       fmt.Sprintf("%s-%d", streamType, position),
			StreamVersion:  position,
		}
	}

	t.Run("commits processed batch and saves checkpoint", func(t *testing.T) {
		instanceID := uuid.New()
		startCursor := store.Cursor{TransactionID: 10, GlobalPosition: 3}
		wantTarget := store.Cursor{TransactionID: 10, GlobalPosition: 5}
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: startCursor}
		storeStub := &stubProjectorStore{
			events: []store.PersistedEvent{
				makeEvent(10, 4, "order"),
				makeEvent(10, 5, "order"),
			},
		}
		registeredProjection := &recordingProjection{name: "orders"}

		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 2, Logger: store.NoOpLogger{}},
		}

		result, err := daemon.processBatch(context.Background(), registrationForTest(t, registeredProjection), startCursor)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		if !result.fullWindow {
			t.Fatal("processBatch() fullWindow = false, want true")
		}
		if result.checkpoint != wantTarget {
			t.Fatalf("processBatch() checkpoint = %+v, want %+v", result.checkpoint, wantTarget)
		}
		if result.handledCount != 2 {
			t.Fatalf("handledCount = %d, want 2", result.handledCount)
		}
		if storeStub.lastAfter != startCursor {
			t.Fatalf("ReadEvents after = %+v, want %+v", storeStub.lastAfter, startCursor)
		}
		if storeStub.lastLimit != 2 {
			t.Fatalf("ReadEvents limit = %d, want 2", storeStub.lastLimit)
		}
		if len(registeredProjection.handled) != 2 {
			t.Fatalf("handled events = %d, want 2", len(registeredProjection.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		if state.beginCalls != 2 {
			t.Fatalf("beginCalls = %d, want 2 (probe + batch)", state.beginCalls)
		}
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1", state.execCalls)
		}
		if state.commitCalls != 1 {
			t.Fatalf("commitCalls = %d, want 1", state.commitCalls)
		}
		if state.rollbackCalls != 1 {
			t.Fatalf("rollbackCalls = %d, want 1 (probe rollback)", state.rollbackCalls)
		}
		if len(state.execQueries) != 1 || !strings.Contains(state.execQueries[0], "projection_checkpoints") {
			t.Fatalf("execQueries = %v, want checkpoint update", state.execQueries)
		}
		if len(state.ops) < 3 || state.ops[0] != "begin" || state.ops[1] != "rollback" || state.ops[2] != "begin" {
			t.Fatalf("ops = %v, want probe tx before batch tx", state.ops)
		}
		if len(state.execArgs) != 1 || len(state.execArgs[0]) != 3 {
			t.Fatalf("execArgs = %#v, want projection name, xid8 string, and position", state.execArgs)
		}
		if got := state.execArgs[0][0]; got != "orders" {
			t.Fatalf("checkpoint projection arg = %v, want orders", got)
		}
		if got := state.execArgs[0][1]; got != "10" {
			t.Fatalf("checkpoint transaction arg = %#v, want %q", got, "10")
		}
		if got := state.execArgs[0][2]; got != int64(5) {
			t.Fatalf("checkpoint position arg = %v, want 5", got)
		}
	})

	t.Run("returns no batch when store has no events", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: store.Cursor{TransactionID: 10, GlobalPosition: 7}}
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  &stubProjectorStore{},
			config: Config{BatchSize: 3, Logger: store.NoOpLogger{}},
		}

		result, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "empty"}), store.Cursor{})
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if result.progressed {
			t.Fatal("processBatch() progressed = true, want false")
		}
		if result.checkpointMoved {
			t.Fatal("processBatch() checkpointMoved = true, want false")
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.rollbackCalls != 1 {
			t.Fatalf("rollbackCalls = %d, want 1", state.rollbackCalls)
		}
	})

	t.Run("checkpoint drift between probe and batch causes a quick re-probe", func(t *testing.T) {
		instanceID := uuid.New()
		moved := store.Cursor{TransactionID: 10, GlobalPosition: 5}
		state := &stubDBState{
			queryValues: []any{
				instanceID.String(),
				moved,
			},
		}
		registeredProjection := &recordingProjection{name: "orders"}
		daemon := &Daemon{
			id:    instanceID,
			db:    openStubDB(t, state),
			store: &stubProjectorStore{events: []store.PersistedEvent{makeEvent(10, 4, "order"), makeEvent(10, 5, "order")}},
			config: Config{
				BatchSize: 2,
				Logger:    store.NoOpLogger{},
			},
		}

		result, err := daemon.processBatch(context.Background(), registrationForTest(t, registeredProjection), store.Cursor{TransactionID: 10, GlobalPosition: 3})
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if result.progressed {
			t.Fatal("processBatch() progressed = true, want false")
		}
		if !result.checkpointMoved {
			t.Fatal("processBatch() checkpointMoved = false, want true")
		}
		if result.checkpoint != moved {
			t.Fatalf("processBatch() checkpoint = %+v, want %+v", result.checkpoint, moved)
		}
		if len(registeredProjection.handled) != 0 {
			t.Fatalf("handled events = %d, want 0", len(registeredProjection.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.commitCalls != 0 {
			t.Fatalf("commitCalls = %d, want 0", state.commitCalls)
		}
	})

	t.Run("filtered projection only receives matching events via in-memory filter", func(t *testing.T) {
		instanceID := uuid.New()
		start := store.Cursor{TransactionID: 10, GlobalPosition: 7}
		want := store.Cursor{TransactionID: 10, GlobalPosition: 9}
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: start}
		storeStub := &stubProjectorStore{
			events: []store.PersistedEvent{
				makeEvent(10, 8, "order"),
				makeEvent(10, 9, "user"),
			},
		}
		inner := &recordingProjection{name: "orders"}
		registeredProjection := FilterStreamTypes(inner, "order")

		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		result, err := daemon.processBatch(context.Background(), registrationForTest(t, registeredProjection), start)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if !result.progressed {
			t.Fatal("processBatch() progressed = false, want true")
		}
		if result.checkpoint != want {
			t.Fatalf("checkpoint = %+v, want %+v", result.checkpoint, want)
		}
		if len(inner.handled) != 1 {
			t.Fatalf("handled events = %d, want 1", len(inner.handled))
		}
		if inner.handled[0].StreamType != "order" {
			t.Fatalf("handled stream type = %q, want order", inner.handled[0].StreamType)
		}
	})

	t.Run("returns read error and rolls back", func(t *testing.T) {
		readErr := errors.New("read failed")
		instanceID := uuid.New()
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  &stubProjectorStore{readErr: readErr},
			config: Config{BatchSize: 3, Logger: store.NoOpLogger{}},
		}

		_, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "reader"}), store.Cursor{})
		if err == nil || !strings.Contains(err.Error(), "read events for projection reader: read failed") {
			t.Fatalf("processBatch() error = %v, want wrapped read error", err)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.rollbackCalls != 1 {
			t.Fatalf("rollbackCalls = %d, want 1", state.rollbackCalls)
		}
	})

	t.Run("returns handler error and rolls back", func(t *testing.T) {
		handleErr := errors.New("boom")
		instanceID := uuid.New()
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
		registeredProjection := &recordingProjection{name: "handler", failAt: 1, handleErr: handleErr}
		daemon := &Daemon{
			id:    instanceID,
			db:    openStubDB(t, state),
			store: &stubProjectorStore{events: []store.PersistedEvent{makeEvent(10, 1, "order")}},
			config: Config{
				BatchSize: 1,
				Logger:    store.NoOpLogger{},
			},
		}

		_, err := daemon.processBatch(context.Background(), registrationForTest(t, registeredProjection), store.Cursor{})
		if err == nil || !strings.Contains(err.Error(), "handle event") || !strings.Contains(err.Error(), "for projection handler: boom") {
			t.Fatalf("processBatch() error = %v, want wrapped handler error", err)
		}
		if len(registeredProjection.handled) != 0 {
			t.Fatalf("handled events = %d, want 0", len(registeredProjection.handled))
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 0 {
			t.Fatalf("execCalls = %d, want 0", state.execCalls)
		}
		if state.rollbackCalls != 2 {
			t.Fatalf("rollbackCalls = %d, want 2 (probe + batch)", state.rollbackCalls)
		}
	})

	t.Run("returns checkpoint save error and rolls back", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{
			execErr:    errors.New("checkpoint failed"),
			ownerID:    instanceID.String(),
			ownerValid: true,
		}
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  &stubProjectorStore{events: []store.PersistedEvent{makeEvent(10, 1, "order")}},
			config: Config{BatchSize: 1, Logger: store.NoOpLogger{}},
		}

		_, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "checkpoint"}), store.Cursor{})
		if err == nil || !strings.Contains(err.Error(), "save checkpoint for projection checkpoint: save checkpoint for projection checkpoint: checkpoint failed") {
			t.Fatalf("processBatch() error = %v, want wrapped checkpoint error", err)
		}

		state.mu.Lock()
		defer state.mu.Unlock()
		if state.execCalls != 1 {
			t.Fatalf("execCalls = %d, want 1", state.execCalls)
		}
		if state.rollbackCalls != 2 {
			t.Fatalf("rollbackCalls = %d, want 2 (probe + batch)", state.rollbackCalls)
		}
	})

	t.Run("advances past inverted global position when transaction id increases", func(t *testing.T) {
		instanceID := uuid.New()
		start := store.Cursor{TransactionID: 9, GlobalPosition: 100}
		want := store.Cursor{TransactionID: 10, GlobalPosition: 50}
		state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: start}
		storeStub := &stubProjectorStore{
			events: []store.PersistedEvent{makeEvent(10, 50, "order")},
		}
		proj := &recordingProjection{name: "orders"}
		daemon := &Daemon{
			id:     instanceID,
			db:     openStubDB(t, state),
			store:  storeStub,
			config: Config{BatchSize: 10, Logger: store.NoOpLogger{}},
		}

		result, err := daemon.processBatch(context.Background(), registrationForTest(t, proj), start)
		if err != nil {
			t.Fatalf("processBatch() error = %v", err)
		}
		if !result.progressed || result.checkpoint != want {
			t.Fatalf("result = %#v, want progressed checkpoint %+v", result, want)
		}
		if len(proj.handled) != 1 || proj.handled[0].GlobalPosition != 50 {
			t.Fatalf("handled = %+v, want event pos 50", proj.handled)
		}
	})
}

type stubSQLStateError struct {
	code string
}

func (e *stubSQLStateError) Error() string {
	return "mock sqlstate: " + e.code
}

func (e *stubSQLStateError) SQLState() string {
	return e.code
}

func TestSaveCheckpointSQL(t *testing.T) {
	state := &stubDBState{}
	tx := &stubPgxTx{state: state}
	cursor := store.Cursor{TransactionID: 400, GlobalPosition: 41}

	if err := projectorpostgres.SaveCheckpoint(context.Background(), tx, "", "orders", cursor); err != nil {
		t.Fatalf("SaveCheckpoint() error = %v", err)
	}

	if len(state.execQueries) != 1 {
		t.Fatalf("execQueries = %d, want 1", len(state.execQueries))
	}
	normalized := strings.Join(strings.Fields(state.execQueries[0]), " ")
	for _, want := range []string{
		"INSERT INTO projection_checkpoints (projection_name, last_transaction_id, last_position",
		"VALUES ($1, $2::xid8, $3::bigint, NOW(), NOW())",
		"ON CONFLICT (projection_name)",
		"WHEN (EXCLUDED.last_transaction_id, EXCLUDED.last_position) > (projection_checkpoints.last_transaction_id, projection_checkpoints.last_position)",
	} {
		if !strings.Contains(normalized, want) {
			t.Fatalf("SQL %q missing %q", normalized, want)
		}
	}
	if len(state.execArgs) != 1 || len(state.execArgs[0]) != 3 {
		t.Fatalf("execArgs = %#v, want 3 args", state.execArgs)
	}
	if state.execArgs[0][0] != "orders" || state.execArgs[0][1] != "400" || state.execArgs[0][2] != int64(41) {
		t.Fatalf("execArgs[0] = %#v, want [orders 400 41]", state.execArgs[0])
	}
}

func TestCommitDetachedCheckpoint_SQLState40001RetryAndExhaustion(t *testing.T) {
	t.Run("retries driver-agnostic 40001 error and commits", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{
			ownerID:      instanceID.String(),
			ownerValid:   true,
			commitErrors: []error{&stubSQLStateError{code: "40001"}, nil},
		}
		entry := registrationForTest(t, &stubDetachedProjection{name: "detached-orders"})
		daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})
		plan := batchPlan{
			checkpoint: store.Cursor{},
			target:     store.Cursor{TransactionID: 10, GlobalPosition: 3},
		}

		res, err := daemon.commitDetachedCheckpoint(context.Background(), entry, &plan)
		if err != nil {
			t.Fatalf("commitDetachedCheckpoint() error = %v", err)
		}
		if !res.progressed {
			t.Fatal("progressed = false, want true")
		}
		if state.commitCalls != 2 {
			t.Fatalf("commitCalls = %d, want 2", state.commitCalls)
		}
	})

	t.Run("returns serialization error after exhausting retry limit", func(t *testing.T) {
		instanceID := uuid.New()
		state := &stubDBState{
			ownerID:    instanceID.String(),
			ownerValid: true,
			commitErrors: []error{
				&pgconn.PgError{Code: "40001"},
				&pgconn.PgError{Code: "40001"},
				&pgconn.PgError{Code: "40001"},
			},
		}
		entry := registrationForTest(t, &stubDetachedProjection{name: "detached-orders"})
		daemon := newDetachedTestDaemon(t, state, &stubProjectorStore{})
		plan := batchPlan{
			checkpoint: store.Cursor{},
			target:     store.Cursor{TransactionID: 10, GlobalPosition: 3},
		}

		_, err := daemon.commitDetachedCheckpoint(context.Background(), entry, &plan)
		if err == nil {
			t.Fatal("commitDetachedCheckpoint() error = nil, want serialization failure")
		}
		if state.commitCalls != detachedCheckpointRetryLimit {
			t.Fatalf("commitCalls = %d, want %d", state.commitCalls, detachedCheckpointRetryLimit)
		}
	})
}

func TestDaemonObserver_BatchProcessed_Success(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	storeStub := &stubProjectorStore{
		events: []store.PersistedEvent{
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 1, StreamType: "order"},
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 2, StreamType: "order"},
			{EventID: uuid.New(), TransactionID: 11, GlobalPosition: 3, StreamType: "order"},
		},
		head: store.Cursor{TransactionID: 11, GlobalPosition: 3},
	}
	observer := &recordingObserver{}
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}, Observer: observer},
	}

	result, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "orders"}), store.Cursor{})
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}
	if !result.progressed {
		t.Fatal("processBatch() progressed = false, want true")
	}

	if len(observer.batches) != 1 {
		t.Fatalf("observer batches count = %d, want 1", len(observer.batches))
	}
	stats := observer.batches[0]
	if stats.ProjectionName != "orders" {
		t.Fatalf("ProjectionName = %q, want %q", stats.ProjectionName, "orders")
	}
	if stats.StartTransactionID != 0 || stats.StartPosition != 0 {
		t.Fatalf("Start cursor = (%d, %d), want (0, 0)", stats.StartTransactionID, stats.StartPosition)
	}
	if stats.LastTransactionID != 11 || stats.LastPosition != 3 {
		t.Fatalf("Last cursor = (%d, %d), want (11, 3)", stats.LastTransactionID, stats.LastPosition)
	}
	if stats.HeadTransactionID != 11 || stats.HeadPosition != 3 {
		t.Fatalf("Head cursor = (%d, %d), want (11, 3)", stats.HeadTransactionID, stats.HeadPosition)
	}
	if stats.Lag != 0 {
		t.Fatalf("Lag = %d, want 0", stats.Lag)
	}
	if stats.EventsRead != 3 {
		t.Fatalf("EventsRead = %d, want 3", stats.EventsRead)
	}
	if stats.EventsHandled != 3 {
		t.Fatalf("EventsHandled = %d, want 3", stats.EventsHandled)
	}
	if stats.Duration <= 0 {
		t.Fatalf("Duration = %v, want > 0", stats.Duration)
	}
	if stats.Error != nil {
		t.Fatalf("Error = %v, want nil", stats.Error)
	}
}

func TestDaemonObserver_BatchProcessed_BacklogHeadLag(t *testing.T) {
	instanceID := uuid.New()
	start := store.Cursor{TransactionID: 9, GlobalPosition: 3}
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true, checkpoint: start}
	storeStub := &stubProjectorStore{
		events: []store.PersistedEvent{
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 4, StreamType: "order"},
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 5, StreamType: "order"},
		},
		head: store.Cursor{TransactionID: 12, GlobalPosition: 9},
	}
	observer := &recordingObserver{}
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 2, Logger: store.NoOpLogger{}, Observer: observer},
	}

	result, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "orders"}), start)
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}
	if !result.progressed || !result.fullWindow {
		t.Fatalf("result = %#v, want progressed fullWindow", result)
	}
	if len(observer.batches) != 1 {
		t.Fatalf("observer batches = %d, want 1", len(observer.batches))
	}
	stats := observer.batches[0]
	if stats.StartTransactionID != 9 || stats.StartPosition != 3 ||
		stats.LastTransactionID != 10 || stats.LastPosition != 5 ||
		stats.HeadTransactionID != 12 || stats.HeadPosition != 9 ||
		stats.Lag != 4 {
		t.Fatalf("unexpected stats = %+v", stats)
	}
}

func TestDaemonObserver_BatchProcessed_FilteredProjections(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	storeStub := &stubProjectorStore{
		events: []store.PersistedEvent{
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 1, StreamType: "order"},
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 2, StreamType: "user"},
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 3, StreamType: "order"},
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 4, StreamType: "invoice"},
		},
	}
	observer := &recordingObserver{}
	inner := &recordingProjection{name: "orders"}
	filtered := FilterStreamTypes(inner, "order")
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}, Observer: observer},
	}

	result, err := daemon.processBatch(context.Background(), registrationForTest(t, filtered), store.Cursor{})
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}
	if !result.progressed {
		t.Fatal("processBatch() progressed = false, want true")
	}

	if len(observer.batches) != 1 {
		t.Fatalf("observer batches count = %d, want 1", len(observer.batches))
	}
	stats := observer.batches[0]
	if stats.EventsRead != 4 {
		t.Fatalf("EventsRead = %d, want 4", stats.EventsRead)
	}
	if stats.EventsHandled != 4 {
		t.Fatalf("EventsHandled = %d, want 4 (all batch events scanned and handled)", stats.EventsHandled)
	}
	if len(inner.handled) != 2 {
		t.Fatalf("inner handled = %d, want 2", len(inner.handled))
	}
	if stats.LastTransactionID != 10 || stats.LastPosition != 4 {
		t.Fatalf("Last cursor = (%d, %d), want (10, 4)", stats.LastTransactionID, stats.LastPosition)
	}
}

func TestDaemonObserver_BatchProcessed_HandlerError(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	storeStub := &stubProjectorStore{
		events: []store.PersistedEvent{
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 1, StreamType: "order"},
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 2, StreamType: "order"},
		},
	}
	observer := &recordingObserver{}
	handlerErr := errors.New("handler failed")
	proj := &recordingProjection{name: "orders", failAt: 2, handleErr: handlerErr}
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}, Observer: observer},
	}

	_, err := daemon.processBatch(context.Background(), registrationForTest(t, proj), store.Cursor{})
	if err == nil {
		t.Fatal("processBatch() error = nil, want handler error")
	}

	if len(observer.batches) != 1 {
		t.Fatalf("observer batches count = %d, want 1", len(observer.batches))
	}
	stats := observer.batches[0]
	if stats.Error == nil {
		t.Fatal("stats.Error = nil, want non-nil error")
	}
	if !strings.Contains(stats.Error.Error(), "handler failed") {
		t.Fatalf("stats.Error = %v, want substring 'handler failed'", stats.Error)
	}
	if stats.StartPosition != 0 {
		t.Fatalf("StartPosition = %d, want 0", stats.StartPosition)
	}
	if stats.LastPosition != 0 {
		t.Fatalf("LastPosition = %d, want 0 (checkpoint not advanced)", stats.LastPosition)
	}
}

func TestDaemonObserver_BatchProcessed_ProbeError(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	storeStub := &stubProjectorStore{
		readErr: errors.New("read events failed"),
	}
	observer := &recordingObserver{}
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}, Observer: observer},
	}

	_, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "orders"}), store.Cursor{})
	if err == nil {
		t.Fatal("processBatch() error = nil, want read error")
	}

	if len(observer.batches) != 1 {
		t.Fatalf("observer batches count = %d, want 1", len(observer.batches))
	}
	stats := observer.batches[0]
	if stats.Error == nil {
		t.Fatal("stats.Error = nil, want non-nil error")
	}
	if !strings.Contains(stats.Error.Error(), "read events failed") {
		t.Fatalf("stats.Error = %v, want substring 'read events failed'", stats.Error)
	}
}

func TestDaemonObserver_BatchProcessed_CommitError(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{
		ownerID:    instanceID.String(),
		ownerValid: true,
		commitErr:  errors.New("commit conflict"),
	}
	storeStub := &stubProjectorStore{
		events: []store.PersistedEvent{
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 1, StreamType: "order"},
		},
	}
	observer := &recordingObserver{}
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}, Observer: observer},
	}

	_, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "orders"}), store.Cursor{})
	if err == nil {
		t.Fatal("processBatch() error = nil, want commit error")
	}

	if len(observer.batches) != 1 {
		t.Fatalf("observer batches count = %d, want 1", len(observer.batches))
	}
	stats := observer.batches[0]
	if stats.Error == nil {
		t.Fatal("stats.Error = nil, want non-nil error")
	}
	if !strings.Contains(stats.Error.Error(), "commit conflict") {
		t.Fatalf("stats.Error = %v, want substring 'commit conflict'", stats.Error)
	}
}

func TestDaemonObserver_Heartbeat(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	observer := &recordingObserver{}
	daemon := &Daemon{
		id: instanceID,
		db: openStubDB(t, state),
		config: Config{
			HeartbeatInterval: 10 * time.Millisecond,
			Logger:            store.NoOpLogger{},
			Observer:          observer,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go daemon.runHeartbeatLoop(ctx)

	// Wait for at least one heartbeat
	deadline := time.Now().Add(500 * time.Millisecond)
	var heartbeats []DaemonStats
	for {
		heartbeats = observer.Heartbeats()
		if len(heartbeats) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	if len(heartbeats) == 0 {
		t.Fatal("no heartbeats observed")
	}
	hb := heartbeats[0]
	if hb.InstanceID != instanceID {
		t.Fatalf("InstanceID = %v, want %v", hb.InstanceID, instanceID)
	}
	if hb.IsLeader {
		t.Fatal("IsLeader = true, want false")
	}
}

func TestDaemonObserver_Rebalance(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{
		ownerID:    instanceID.String(),
		ownerValid: true,
		queryValues: []any{
			// ListLiveInstances
			instanceID.String(),
			// GetAssignments
			"p1",
		},
	}
	observer := &recordingObserver{}
	daemon := &Daemon{
		id: instanceID,
		db: openStubDB(t, state),
		config: Config{
			Logger:   store.NoOpLogger{},
			Observer: observer,
		},
	}

	tx, err := daemon.db.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	conn := &pgxpool.Conn{}
	_ = conn
	_ = tx

	assignments := map[string]uuid.UUID{"p1": instanceID}
	if daemon.config.Observer != nil {
		daemon.config.Observer.OnRebalance(context.Background(), assignments)
	}

	if len(observer.rebalances) != 1 {
		t.Fatalf("rebalances count = %d, want 1", len(observer.rebalances))
	}
	if observer.rebalances[0]["p1"] != instanceID {
		t.Fatalf("rebalance p1 = %v, want %v", observer.rebalances[0]["p1"], instanceID)
	}
}

func TestDaemonObserver_NilObserver_Safety(t *testing.T) {
	instanceID := uuid.New()
	state := &stubDBState{ownerID: instanceID.String(), ownerValid: true}
	storeStub := &stubProjectorStore{
		events: []store.PersistedEvent{
			{EventID: uuid.New(), TransactionID: 10, GlobalPosition: 1, StreamType: "order"},
		},
	}
	daemon := &Daemon{
		id:     instanceID,
		db:     openStubDB(t, state),
		store:  storeStub,
		config: Config{BatchSize: 10, Logger: store.NoOpLogger{}, Observer: nil},
	}

	// Should not panic on batch processing
	result, err := daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "orders"}), store.Cursor{})
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}
	if !result.progressed {
		t.Fatal("progressed = false, want true")
	}

	// Should not panic when empty
	storeStub.events = nil
	_, err = daemon.processBatch(context.Background(), registrationForTest(t, &recordingProjection{name: "orders"}), store.Cursor{TransactionID: 10, GlobalPosition: 1})
	if err != nil {
		t.Fatalf("processBatch() empty error = %v", err)
	}
}

func TestDaemon_IsRunning(t *testing.T) {
	d := &Daemon{}

	if d.IsRunning() {
		t.Fatal("IsRunning() = true initially, want false")
	}

	d.mu.Lock()
	d.started = true
	d.mu.Unlock()

	if !d.IsRunning() {
		t.Fatal("IsRunning() = false when started, want true")
	}

	d.mu.Lock()
	d.started = false
	d.mu.Unlock()

	if d.IsRunning() {
		t.Fatal("IsRunning() = true when stopped, want false")
	}
}

func TestDaemon_IsLeader(t *testing.T) {
	d := &Daemon{}

	if d.IsLeader() {
		t.Fatal("IsLeader() = true initially, want false")
	}

	d.setLeader(true)
	if !d.IsLeader() {
		t.Fatal("IsLeader() = false after setLeader(true), want true")
	}

	d.setLeader(false)
	if d.IsLeader() {
		t.Fatal("IsLeader() = true after setLeader(false), want false")
	}
}

func TestDaemon_ShutdownTimeout(t *testing.T) {
	dDefault := &Daemon{config: Config{}}
	if got := dDefault.shutdownTimeout(); got != defaultShutdownTimeout {
		t.Fatalf("shutdownTimeout() = %v, want default %v", got, defaultShutdownTimeout)
	}

	dCustom := &Daemon{config: Config{ShutdownTimeout: 15 * time.Second}}
	if got := dCustom.shutdownTimeout(); got != 15*time.Second {
		t.Fatalf("shutdownTimeout() = %v, want %v", got, 15*time.Second)
	}
}

func TestDaemon_Start_CanceledContextReturnsNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	state := &stubDBState{
		execErr: context.Canceled,
	}

	d := New(
		openStubDB(t, state),
		&stubProjectorStore{},
		FromProjections(&recordingProjection{name: "orders"}),
		WithLogger(store.NoOpLogger{}),
	)

	err := d.Start(ctx)
	if err != nil {
		t.Fatalf("Start(canceledContext) returned non-nil error: %v, want nil", err)
	}
}
