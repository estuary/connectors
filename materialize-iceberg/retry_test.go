package connector

import (
	"errors"
	"fmt"
	"testing"

	ssmTypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassifySSMPut(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"too many updates", fmt.Errorf("operation error SSM: PutParameter: %w", &ssmTypes.TooManyUpdates{}), retry.Retryable},
		{"parameter limit exceeded", &ssmTypes.ParameterLimitExceeded{}, retry.Fatal},
		{"other error", errors.New("boom"), retry.Fatal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifySSMPut(tt.err))
		})
	}
}
