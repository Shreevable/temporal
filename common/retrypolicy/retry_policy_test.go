package retrypolicy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestIsRetryableFailure(t *testing.T) {
	testCases := []struct {
		name              string
		failure           *failurepb.Failure
		nonRetryableTypes []string
		retryable         bool
	}{
		{
			name:      "nil failure",
			retryable: true,
		},
		{
			name: "terminated failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}},
			},
		},
		{
			name: "canceled failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}},
			},
		},
		{
			name: "unspecified timeout",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_UNSPECIFIED,
				}},
			},
		},
		{
			name: "start-to-close timeout",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				}},
			},
			retryable: true,
		},
		{
			name: "start-to-close timeout excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				}},
			},
			nonRetryableTypes: []string{TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()},
		},
		{
			name: "schedule-to-start timeout",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				}},
			},
		},
		{
			name: "schedule-to-start timeout excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				}},
			},
			nonRetryableTypes: []string{TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String()},
		},
		{
			name: "schedule-to-close timeout",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
				}},
			},
		},
		{
			name: "schedule-to-close timeout excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
				}},
			},
			nonRetryableTypes: []string{TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()},
		},
		{
			name: "heartbeat timeout",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
				}},
			},
			retryable: true,
		},
		{
			name: "heartbeat timeout excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
				}},
			},
			nonRetryableTypes: []string{TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_HEARTBEAT.String()},
		},
		{
			name: "heartbeat timeout with different timeout excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
				}},
			},
			nonRetryableTypes: []string{TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()},
			retryable:         true,
		},
		{
			name: "heartbeat timeout with unknown timeout excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
				}},
			},
			nonRetryableTypes: []string{TimeoutFailureTypePrefix + "unknown timeout type string"},
			retryable:         true,
		},
		{
			name: "retryable server failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{}},
			},
			retryable: true,
		},
		{
			name: "non-retryable server failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: true}},
			},
		},
		{
			name: "application failure marked non-retryable",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true}},
			},
		},
		{
			name: "retryable application failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "type"}},
			},
			retryable: true,
		},
		{
			name: "application failure with different type excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "type"}},
			},
			nonRetryableTypes: []string{"otherType"},
			retryable:         true,
		},
		{
			name: "application failure type excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "type"}},
			},
			nonRetryableTypes: []string{"otherType", "type"},
		},
		{
			name: "application failure type solely excluded",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "type"}},
			},
			nonRetryableTypes: []string{"type"},
		},
		{
			name: "child workflow failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{}},
				Cause: &failurepb.Failure{
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true}},
				},
			},
			retryable: true,
		},
		{
			name: "child workflow failure containing activity failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{}},
				Cause: &failurepb.Failure{
					FailureInfo: &failurepb.Failure_ActivityFailureInfo{ActivityFailureInfo: &failurepb.ActivityFailureInfo{}},
					Cause: &failurepb.Failure{
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: true}},
					},
				},
			},
			retryable: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.retryable, IsRetryableFailure(tc.failure, tc.nonRetryableTypes))
		})
	}
}

func TestEnsureRetryPolicyDefaults(t *testing.T) {
	defaultRetrySettings := DefaultRetrySettings{
		InitialInterval:            time.Second,
		MaximumIntervalCoefficient: 100,
		BackoffCoefficient:         2.0,
		MaximumAttempts:            120,
	}

	defaultRetryPolicy := &commonpb.RetryPolicy{
		InitialInterval:    durationpb.New(1 * time.Second),
		MaximumInterval:    durationpb.New(100 * time.Second),
		BackoffCoefficient: 2.0,
		MaximumAttempts:    120,
	}

	testCases := []struct {
		name  string
		input *commonpb.RetryPolicy
		want  *commonpb.RetryPolicy
	}{
		{
			name:  "default fields are set ",
			input: &commonpb.RetryPolicy{},
			want:  defaultRetryPolicy,
		},
		{
			name: "non-default InitialIntervalInSeconds is set, MaximumAttempts stays unset (0)",
			input: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(2 * time.Second),
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(2 * time.Second),
				MaximumInterval:    durationpb.New(200 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    0,
			},
		},
		{
			name: "non-default MaximumIntervalInSeconds is set, MaximumAttempts stays unset (0)",
			input: &commonpb.RetryPolicy{
				MaximumInterval: durationpb.New(1000 * time.Second),
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(1000 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    0,
			},
		},
		{
			name: "non-default BackoffCoefficient is set, MaximumAttempts stays unset (0)",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1.5,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(100 * time.Second),
				BackoffCoefficient: 1.5,
				MaximumAttempts:    0,
			},
		},
		{
			name: "non-default Maximum attempts is not set",
			input: &commonpb.RetryPolicy{
				MaximumAttempts: 49,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(100 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    49,
			},
		},
		{
			name: "non-retryable errors are set, MaximumAttempts stays unset (0)",
			input: &commonpb.RetryPolicy{
				NonRetryableErrorTypes: []string{"testFailureType"},
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:        durationpb.New(1 * time.Second),
				MaximumInterval:        durationpb.New(100 * time.Second),
				BackoffCoefficient:     2.0,
				MaximumAttempts:        0,
				NonRetryableErrorTypes: []string{"testFailureType"},
			},
		},
		{
			name:  "empty policy has non-zero defaults",
			input: &commonpb.RetryPolicy{},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(100 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    120,
			},
		},
		{
			name: "explicit unlimited (-1) survives a non-zero default",
			input: &commonpb.RetryPolicy{
				MaximumAttempts: -1,
			},
			want: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(1 * time.Second),
				MaximumInterval:    durationpb.New(100 * time.Second),
				BackoffCoefficient: 2,
				MaximumAttempts:    -1,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			EnsureDefaults(tt.input, defaultRetrySettings)
			assert.Equal(t, tt.want, tt.input)
		})
	}
}

func TestValidateRetryPolicy(t *testing.T) {
	testCases := []struct {
		name          string
		input         *commonpb.RetryPolicy
		wantErr       bool
		wantErrString string
	}{
		{
			name:          "nil policy is okay",
			input:         nil,
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "maxAttempts is 1, coefficient < 1",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 0.5,
				MaximumAttempts:    1,
			},
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "initial interval negative",
			input: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(-22 * time.Second),
			},
			wantErr:       true,
			wantErrString: "invalid InitialInterval set on retry policy: negative duration",
		},
		{
			name: "coefficient < 1",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 0.8,
			},
			wantErr:       true,
			wantErrString: "BackoffCoefficient cannot be less than 1 on retry policy.",
		},
		{
			name: "maximum interval in seconds is negative",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 2.0,
				MaximumInterval:    durationpb.New(-2 * time.Second),
			},
			wantErr:       true,
			wantErrString: "invalid MaximumInterval set on retry policy: negative duration",
		},
		{
			name: "maximum interval in less than initial interval",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 2.0,
				MaximumInterval:    durationpb.New(5 * time.Second),
				InitialInterval:    durationpb.New(10 * time.Second),
			},
			wantErr:       true,
			wantErrString: "MaximumInterval cannot be less than InitialInterval on retry policy.",
		},
		{
			name: "maximum attempts negative",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 2.0,
				MaximumAttempts:    -3,
			},
			wantErr:       true,
			wantErrString: "MaximumAttempts cannot be negative on retry policy.",
		},
		{
			name: "maximum attempts -1 is a valid unlimited sentinel",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 2.0,
				MaximumAttempts:    -1,
			},
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "timeout nonretryable error - valid type",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_HEARTBEAT.String(),
				},
			},
			wantErr:       false,
			wantErrString: "",
		},
		{
			name: "timeout nonretryable error - unspecified type",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_UNSPECIFIED.String(),
				},
			},
			wantErr:       true,
			wantErrString: "Invalid timeout type value: Unspecified.",
		},
		{
			name: "timeout nonretryable error - unknown type",
			input: &commonpb.RetryPolicy{
				BackoffCoefficient: 1,
				NonRetryableErrorTypes: []string{
					TimeoutFailureTypePrefix + "unknown",
				},
			},
			wantErr:       true,
			wantErrString: "Invalid timeout type value: unknown.",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.input)
			if tt.wantErr {
				assert.NotNil(t, err, "expected error - did not get one")
				assert.Equal(t, err.Error(), tt.wantErrString, "unexpected error message")
			} else {
				assert.Nil(t, err, "unexpected error")
			}
		})
	}
}

// TestIssue11721Fix mirrors https://github.com/temporalio/temporal/issues/11721.
func TestIssue11721Fix(t *testing.T) {
	namespaceDefault := DefaultRetrySettings{
		InitialInterval:            DefaultDefaultRetrySettings.InitialInterval,
		MaximumIntervalCoefficient: DefaultDefaultRetrySettings.MaximumIntervalCoefficient,
		BackoffCoefficient:         DefaultDefaultRetrySettings.BackoffCoefficient,
		MaximumAttempts:            5,
	}

	t.Run("case A: reporter repro - explicit MaximumAttempts=0 alongside other fields stays 0", func(t *testing.T) {
		policy := &commonpb.RetryPolicy{
			BackoffCoefficient: 3.0,
			MaximumAttempts:    0,
		}
		EnsureDefaults(policy, namespaceDefault)
		require.NoError(t, Validate(policy))
		require.Equal(t, 3.0, policy.GetBackoffCoefficient())
		require.Equal(t, int32(0), policy.GetMaximumAttempts())
	})

	t.Run("case B: fully empty policy still gets MaximumAttempts filled from the default", func(t *testing.T) {
		policy := &commonpb.RetryPolicy{}
		EnsureDefaults(policy, namespaceDefault)
		require.NoError(t, Validate(policy))
		require.Equal(t, int32(5), policy.GetMaximumAttempts())
	})

	t.Run("case C: MaximumAttempts=-1 still survives unchanged", func(t *testing.T) {
		policy := &commonpb.RetryPolicy{
			BackoffCoefficient: 3.0,
			MaximumAttempts:    -1,
		}
		EnsureDefaults(policy, namespaceDefault)
		require.NoError(t, Validate(policy))
		require.Equal(t, int32(-1), policy.GetMaximumAttempts())
	})
}
