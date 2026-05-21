package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmTypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	"github.com/estuary/connectors/go/blob"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/materialize-iceberg/python"
	"github.com/google/uuid"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
)

type emrClient struct {
	cfg                 emrConfig
	catalogAuth         catalogAuthConfig
	catalogURL          string
	warehouse           string
	materializationName string
	c                   *emr.Client
	bucket              blob.Bucket
	ssmClient           *ssm.Client
	tokenURL            string

	// Set after StartJobRun fails with an access-denied error while passing
	// Tags. Subsequent runs in this process will skip tagging to avoid
	// repeating the failure. Customers can grant `emr-serverless:TagResource`
	// to enable tagging.
	tagsDisabled bool
}

func (e *emrClient) checkPrereqs(ctx context.Context, errs *cerrors.PrereqErr) {
	if _, err := e.c.ListJobRuns(ctx, &emr.ListJobRunsInput{
		ApplicationId: aws.String(e.cfg.ApplicationId),
		MaxResults:    aws.Int32(1),
	}); err != nil {
		errs.Err(fmt.Errorf("failed to list job runs for application %q: %w", e.cfg.ApplicationId, err))
	}

	if e.catalogAuth.AuthType == catalogAuthTypeClientCredential {
		testParameter := e.cfg.SystemsManagerPrefix + "test"
		if err := ssmPutParameterWithRetry(ctx, e.ssmClient, testParameter, "test"); err != nil {
			errs.Err(fmt.Errorf("failed to put secure string parameter to %s: %w", testParameter, err))
			return
		} else if _, err := e.ssmClient.GetParameter(ctx, &ssm.GetParameterInput{
			Name:           aws.String(testParameter),
			WithDecryption: aws.Bool(true),
		}); err != nil {
			errs.Err(fmt.Errorf("failed to get secure string parameter %s: %w", testParameter, err))
		}
	}
}

func (e *emrClient) ensureSecret(ctx context.Context, wantCred string) error {
	res, err := e.ssmClient.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(e.clientCredSecretName()),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		var errNotFound *ssmTypes.ParameterNotFound
		if !errors.As(err, &errNotFound) {
			return fmt.Errorf("error getting catalog credential parameter: %w", err)
		}
	}

	if res != nil && *res.Parameter.Value == wantCred {
		return nil
	}

	if err := ssmPutParameterWithRetry(ctx, e.ssmClient, e.clientCredSecretName(), wantCred); err != nil {
		return fmt.Errorf("error writing catalog credential parameter: %w", err)
	}

	log.Info("catalog credential parameter created")

	return nil
}

func (e *emrClient) runJob(ctx context.Context, input any, entryPointUri, pyFilesCommonURI, jobName, workingPrefix string) error {
	/***
	Available arguments to the pyspark script:
	| --input-uri              | Input for the program, as an s3 URI, to be parsed by the script                      | Required |
	| --status-output.         | Location where the final status object will be written.                              | Required |
	| --catalog-url            | The catalog URL                                                                      | Required |
	| --warehouse              | REST Warehouse                                                                       | Required |
	| --region                 | EMR & SigV4 Region                                                                   | Required |
	| --credential-secret-name | Name of the secret in Systems Manager if using client credentials auth               | Optional |
	| --scope                  | Scope if using client credentials auth                                               | Optional |
	| --signing-name           | Signing name to use when authenticating with AWS SigV4. Either 'glue' or 's3tables'. | Optional |
	| --oauth2-server-uri      | OAuth2 token endpoint URI.                                                           | Optional |
	***/
	getStatus := func() (*python.StatusOutput, error) {
		var status python.StatusOutput
		statusKey := path.Join(workingPrefix, statusFile)
		if statusObj, err := e.bucket.NewReader(ctx, statusKey); err != nil {
			return nil, fmt.Errorf("reading status object %q: %w", statusKey, err)
		} else if err := json.NewDecoder(statusObj).Decode(&status); err != nil {
			return nil, fmt.Errorf("decoding status object %q: %w", statusKey, err)
		} else if err := statusObj.Close(); err != nil {
			return nil, fmt.Errorf("closing status object reader %q: %w", statusKey, err)
		}
		return &status, nil
	}

	inputKey := path.Join(workingPrefix, "input.json")
	if inputBytes, err := encodeInput(input); err != nil {
		return fmt.Errorf("encoding input: %w", err)
	} else if err := e.bucket.Upload(ctx, inputKey, bytes.NewReader(inputBytes)); err != nil {
		return fmt.Errorf("putting input file object: %w", err)
	}

	args := []string{
		"--input-uri", "s3://" + path.Join(e.cfg.Bucket, inputKey),
		"--status-output", "s3://" + path.Join(e.cfg.Bucket, workingPrefix, statusFile),
		"--catalog-url", e.catalogURL,
		"--warehouse", e.warehouse,
		"--region", e.cfg.Region,
	}

	if e.tokenURL != "" {
		args = append(args, "--oauth2-server-uri", e.tokenURL)
	}

	if e.catalogAuth.AuthType == catalogAuthTypeClientCredential {
		args = append(args, "--credential-secret-name", e.clientCredSecretName())
		if s := e.catalogAuth.Scope; s != "" {
			args = append(args, "--scope", s)
		}
	} else if e.catalogAuth.AuthType == catalogAuthTypeSigV4 || e.catalogAuth.AuthType == catalogAuthTypeAWSIAM {
		signingName, err := SigningName(e.catalogURL)
		if err != nil {
			return err
		}
		args = append(args, "--signing-name", signingName)
	}

	startInput := &emr.StartJobRunInput{
		ApplicationId:    aws.String(e.cfg.ApplicationId),
		ClientToken:      aws.String(uuid.NewString()),
		ExecutionRoleArn: aws.String(e.cfg.ExecutionRoleArn),
		JobDriver: &emrTypes.JobDriverMemberSparkSubmit{
			Value: emrTypes.SparkSubmit{
				SparkSubmitParameters: aws.String(fmt.Sprintf("--py-files %s --conf spark.driver.maxResultSize=0 --conf spark.sql.iceberg.vectorization.enabled=false", pyFilesCommonURI)),
				EntryPoint:            aws.String(entryPointUri),
				EntryPointArguments:   args,
			},
		},
		Name: aws.String(jobName),
	}
	if !e.tagsDisabled {
		startInput.Tags = map[string]string{"estuary:materialization": e.materializationName}
	}

	start, err := e.c.StartJobRun(ctx, startInput)
	if err != nil {
		// AccessDenied likely means the execution role lacks
		// `emr-serverless:TagResource`. Disable tagging for the lifetime of
		// this client and retry without tags so the materialization can keep
		// running on existing IAM policies.
		if startInput.Tags != nil && isAccessDeniedErr(err) {
			log.WithError(err).Warn("StartJobRun denied while passing tags; retrying without tags. Grant 'emr-serverless:TagResource' to the execution role to enable cost-allocation tagging.")
			e.tagsDisabled = true
			startInput.Tags = nil
			startInput.ClientToken = aws.String(uuid.NewString())
			start, err = e.c.StartJobRun(ctx, startInput)
		}
		if err != nil {
			return err
		}
	}

	var runDetails string
	for {
		gotRun, err := e.c.GetJobRun(ctx, &emr.GetJobRunInput{
			ApplicationId: aws.String(e.cfg.ApplicationId),
			JobRunId:      start.JobRunId,
		})
		if err != nil {
			return err
		}
		runDetails = *gotRun.JobRun.StateDetails

		switch gotRun.JobRun.State {
		case emrTypes.JobRunStateSuccess:
			if status, err := getStatus(); err != nil {
				return fmt.Errorf("job succeeded but could not get final status: %w", err)
			} else if !status.Success {
				return fmt.Errorf("job failed ran successfully but had error: %s", status.Error)
			} else {
				python.ForwardLogs(status.Logs)
			}

			return nil
		case emrTypes.JobRunStateFailed, emrTypes.JobRunStateCancelling, emrTypes.JobRunStateCancelled:
			if status, err := getStatus(); err != nil {
				return fmt.Errorf("job failed with no status output: %s", runDetails)
			} else {
				python.ForwardLogs(status.Logs)
				log.WithField("runDetails", runDetails).Error("emr job failed")
				return fmt.Errorf("job failed: %s", status.Error)
			}
		default:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				continue
			}
		}
	}
}

func (e *emrClient) clientCredSecretName() string {
	return e.cfg.SystemsManagerPrefix + sanitizeAndAppendHash(e.materializationName)
}

func ssmPutParameterWithRetry(ctx context.Context, client *ssm.Client, name, value string) error {
	var err error
	for attempt := range 5 {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      aws.String(name),
			Value:     aws.String(value),
			Type:      ssmTypes.ParameterTypeSecureString,
			Overwrite: aws.Bool(true),
		})
		if err == nil {
			return nil
		}
		var tooMany *ssmTypes.TooManyUpdates
		if !errors.As(err, &tooMany) {
			return err
		}
		log.WithField("attempt", attempt+1).Warn("SSM PutParameter TooManyUpdates, retrying")
	}
	return err
}

func isAccessDeniedErr(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "AccessDeniedException", "AccessDenied":
		return true
	}
	return false
}

func encodeInput(in any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetAppendNewline(false)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(in); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
