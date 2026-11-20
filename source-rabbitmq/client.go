package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	mysqltls "github.com/estuary/connectors/go/mysql/tls"
	amqp "github.com/rabbitmq/amqp091-go"
)

const connectTimeout = 30 * time.Second

func buildClient(ctx context.Context, cfg *config) (*amqp.Connection, error) {
	options, err := clientConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	scheme := "amqp"
	if cfg.TLS {
		scheme = "amqps"
	}
	endpoint := url.URL{Scheme: scheme, Host: net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))}

	client, err := amqp.DialConfig(endpoint.String(), options)
	if err != nil {
		return nil, connectionError(cfg, err)
	}
	return client, nil
}

func connectionError(cfg *config, err error) error {
	address := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	var protocolError *amqp.Error
	plainMismatch := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	if errors.As(err, &protocolError) {
		switch protocolError.Code {
		case amqp.FrameError:
			// amqp091-go also wraps socket failures as frame errors.
			reason := strings.ToLower(protocolError.Reason)
			plainMismatch = strings.Contains(reason, "frame") || strings.Contains(reason, "unexpected bytes") ||
				reason == "eof" || reason == "unexpected eof"
		case amqp.SyntaxError, amqp.CommandInvalid, amqp.UnexpectedFrame:
			plainMismatch = true
		}
	}
	if !cfg.TLS && plainMismatch {
		return fmt.Errorf("connecting to RabbitMQ: the server did not speak plain AMQP on %s; it may require TLS (enable TLS, or use port 5672 for non-TLS): %w", address, err)
	}
	if cfg.TLS && strings.Contains(err.Error(), "tls: first record does not look like a TLS handshake") {
		return fmt.Errorf("connecting to RabbitMQ: the server did not speak TLS on %s; it may require plain AMQP (disable TLS, or use port 5671 for TLS): %w", address, err)
	}
	return fmt.Errorf("connecting to RabbitMQ: %w", err)
}

func clientConfig(ctx context.Context, cfg *config) (amqp.Config, error) {
	tlsConfig, err := cfg.buildTLSConfig()
	if err != nil {
		return amqp.Config{}, err
	}
	var auth amqp.Authentication = &amqp.PlainAuth{Username: cfg.Username, Password: cfg.Password}
	if cfg.AuthMechanism == "EXTERNAL" {
		auth = &amqp.ExternalAuth{}
	}
	return amqp.Config{
		SASL:            []amqp.Authentication{auth},
		Vhost:           cfg.VirtualHost,
		TLSClientConfig: tlsConfig,
		Dial: func(network, addr string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: connectTimeout}
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}

			// Bound the TLS and AMQP handshakes as well as the TCP dial.
			// amqp091-go clears this deadline once the handshake completes.
			deadline := time.Now().Add(connectTimeout)
			if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
				deadline = ctxDeadline
			}
			if err := conn.SetDeadline(deadline); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		},
	}, nil
}

func (c *config) buildTLSConfig() (*tls.Config, error) {
	if !c.TLS {
		if c.TLSConfig != (tlsConfig{}) {
			return nil, errors.New("'tlsConfig' requires 'tls' to be enabled")
		}
		return nil, nil
	}
	if c.TLSConfig == (tlsConfig{}) {
		return &tls.Config{ServerName: c.Host}, nil
	}
	settings := mysqltls.Settings{
		Mode:       mysqltls.ModeVerifyIdentity,
		ServerCA:   c.TLSConfig.SSLServerCA,
		ClientCert: c.TLSConfig.SSLClientCert,
		ClientKey:  c.TLSConfig.SSLClientKey,
	}
	tlsConfig, err := settings.Config(c.Host)
	if err != nil {
		return nil, fmt.Errorf("invalid 'tlsConfig': %w", err)
	}
	// Retain system-root trust when only a client certificate is configured.
	if settings.ServerCA == "" {
		tlsConfig.RootCAs = nil
	}
	return tlsConfig, nil
}

func buildHTTPClient(cfg *config) (*http.Client, error) {
	tlsConfig, err := cfg.buildTLSConfig()
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &http.Client{
		Transport: transport,
		Timeout:   connectTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}
