package persistence

import "errors"

var (
	// ErrNotFound indicates no persisted state record exists yet for the requested target ID.
	ErrNotFound = errors.New("persistence: state not found")

	// ErrConcurrencyConflict indicates an optimistic concurrency conflict during persistence.
	ErrConcurrencyConflict = errors.New("persistence: concurrency conflict")

	// ErrInvalidVersion indicates a loaded stream version was negative.
	ErrInvalidVersion = errors.New("persistence: stream version must be non-negative")

	// ErrNilRunner indicates a nil or typed-nil Runner was supplied.
	ErrNilRunner = errors.New("persistence: runner cannot be nil")

	// ErrNilRepository indicates a nil or typed-nil repository was supplied.
	ErrNilRepository = errors.New("persistence: repository cannot be nil")

	// ErrNilInitial indicates a nil Initial function was supplied on Decider.
	ErrNilInitial = errors.New("persistence: initial func cannot be nil")

	// ErrNilEvolve indicates a nil Evolve function was supplied on Decider.
	ErrNilEvolve = errors.New("persistence: evolve func cannot be nil")

	// ErrNilTargetID indicates a nil target ID extractor function was supplied.
	ErrNilTargetID = errors.New("persistence: idOf func cannot be nil")

	// ErrNilDecide indicates a nil decision function was supplied.
	ErrNilDecide = errors.New("persistence: decide func cannot be nil")

	// ErrEmptyTargetID indicates the extracted target identity was empty or whitespace.
	ErrEmptyTargetID = errors.New("persistence: target ID must not be blank")

	// ErrNilContext indicates a nil context.Context was supplied.
	ErrNilContext = errors.New("persistence: context cannot be nil")
)
