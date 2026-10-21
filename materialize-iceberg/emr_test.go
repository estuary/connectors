package connector

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

type fakeApplicationGetter struct {
	releaseLabel string
	err          error
}

func (f fakeApplicationGetter) GetApplication(context.Context, *emr.GetApplicationInput, ...func(*emr.Options)) (*emr.GetApplicationOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &emr.GetApplicationOutput{Application: &emrTypes.Application{ReleaseLabel: aws.String(f.releaseLabel)}}, nil
}

func TestCheckVariantReleaseLabel(t *testing.T) {
	accessDenied := &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not allowed"}

	for _, tt := range []struct {
		name    string
		getter  fakeApplicationGetter
		wantErr string
	}{
		{"emr 7 fails", fakeApplicationGetter{releaseLabel: "emr-7.9.0"}, "need Spark 4"},
		{"emr 6 fails", fakeApplicationGetter{releaseLabel: "emr-6.15.0"}, "need Spark 4"},
		{"emr-spark-8.0.0 passes", fakeApplicationGetter{releaseLabel: "emr-spark-8.0.0"}, ""},
		{"emr-spark-8.1.0 passes", fakeApplicationGetter{releaseLabel: "emr-spark-8.1.0"}, ""},
		{"emr-spark-9.0.0 passes", fakeApplicationGetter{releaseLabel: "emr-spark-9.0.0"}, ""},
		{"emr 7 preview fails", fakeApplicationGetter{releaseLabel: "emr-7.0.0-preview"}, "need Spark 4"},
		{"emr-spark-8 preview passes", fakeApplicationGetter{releaseLabel: "emr-spark-8.1.0-preview"}, ""},
		{"unparseable label is tolerated", fakeApplicationGetter{releaseLabel: "custom"}, ""},
		{"access denied is tolerated", fakeApplicationGetter{err: accessDenied}, ""},
		{"other errors fail", fakeApplicationGetter{err: errors.New("boom")}, "getting EMR application"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := checkVariantReleaseLabel(context.Background(), tt.getter, "app-1")
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
