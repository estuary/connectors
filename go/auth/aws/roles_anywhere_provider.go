package aws

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	helper "github.com/aws/rolesanywhere-credential-helper/aws_signing_helper"
)

type rolesAnywhereProvider struct {
	ClientPrivateKey  string
	ClientCertificate string
	TrustAnchorArnStr string
	ProfileArnStr     string
	RoleArn           string
}

func (r *rolesAnywhereProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	keyFile, err := os.CreateTemp("", "clientPrivateKey")
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("creating client private key file: %w", err)
	}
	defer os.Remove(keyFile.Name())

	certFile, err := os.CreateTemp("", "clientCertificate")
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("creating client certificate file: %w", err)
	}
	defer os.Remove(certFile.Name())

	if _, err := keyFile.WriteString(r.ClientPrivateKey); err != nil {
		return aws.Credentials{}, fmt.Errorf("writing client private key: %w", err)
	} else if err := keyFile.Close(); err != nil {
		return aws.Credentials{}, fmt.Errorf("closing client private key file: %w", err)
	} else if _, err := certFile.WriteString(r.ClientCertificate); err != nil {
		return aws.Credentials{}, fmt.Errorf("writing client certificate: %w", err)
	} else if err := certFile.Close(); err != nil {
		return aws.Credentials{}, fmt.Errorf("closing client certificate file: %w", err)
	}

	opts := &helper.CredentialsOpts{
		PrivateKeyId:      keyFile.Name(),
		CertificateId:     certFile.Name(),
		TrustAnchorArnStr: r.TrustAnchorArnStr,
		ProfileArnStr:     r.ProfileArnStr,
		RoleArn:           r.RoleArn,
		SessionDuration:   3600,
	}

	signer, signingAlgorithm, err := helper.GetSigner(opts)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("getting signer: %w", err)
	}

	out, err := helper.GenerateCredentials(opts, signer, signingAlgorithm)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("generating credentials: %w", err)
	}

	expires, err := time.Parse(time.RFC3339Nano, out.Expiration)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("parsing expiration time: %w", err)
	}

	return aws.Credentials{
		AccessKeyID:     out.AccessKeyId,
		SecretAccessKey: out.SecretAccessKey,
		SessionToken:    out.SessionToken,
		CanExpire:       true,
		Expires:         expires,
	}, nil
}
