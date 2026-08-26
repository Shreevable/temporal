package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// StickyWorkerUnavailable represents sticky worker unavailable error.
	StickyWorkerUnavailable struct {
		Message string
		// DefinitelyUnavailable is true if the sticky worker gracefully shut down and will not
		// return, as opposed to merely having no recent poller.
		DefinitelyUnavailable bool
		st                    *status.Status
	}
)

// NewStickyWorkerUnavailable returns new StickyWorkerUnavailable error for the ambiguous
// case, where the sticky worker merely has no recent poller and might still come back.
func NewStickyWorkerUnavailable() error {
	return &StickyWorkerUnavailable{
		Message: "sticky worker unavailable, please use original task queue.",
	}
}

// NewStickyWorkerShutdown returns new StickyWorkerUnavailable error for the case where the
// server knows with certainty that the sticky worker was gracefully shut down and will not
// come back.
func NewStickyWorkerShutdown() error {
	return &StickyWorkerUnavailable{
		Message:               "sticky worker unavailable, please use original task queue.",
		DefinitelyUnavailable: true,
	}
}

// Error returns string message.
func (e *StickyWorkerUnavailable) Error() string {
	return e.Message
}

func (e *StickyWorkerUnavailable) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Unavailable, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.StickyWorkerUnavailableFailure{
			DefinitelyUnavailable: e.DefinitelyUnavailable,
		},
	)
	return st
}

func newStickyWorkerUnavailable(st *status.Status, failure *errordetailsspb.StickyWorkerUnavailableFailure) error {
	return &StickyWorkerUnavailable{
		Message:               st.Message(),
		DefinitelyUnavailable: failure.GetDefinitelyUnavailable(),
		st:                    st,
	}
}
