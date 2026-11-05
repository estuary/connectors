package connector

import (
	"errors"
	"time"

	ssmTypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/estuary/connectors/go/retry"
)

// ssmPutRetry retries writing a Systems Manager parameter while SSM refuses
// the write because another update to the same parameter is in progress.
var ssmPutRetry = retry.Policy{
	Classify:       classifySSMPut,
	MaxAttempts:    5,
	InitialBackoff: 2 * time.Second,
	MaxBackoff:     8 * time.Second,
	Jitter:         0.5,
}

func classifySSMPut(err error) retry.Outcome {
	if _, ok := errors.AsType[*ssmTypes.TooManyUpdates](err); ok {
		return retry.Retryable
	}
	return retry.Fatal
}
