package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

//go:embed VERSION
var rawVersion string

func productVersion() string {
	return regexp.MustCompile(`^v(\d+)\s*$`).FindStringSubmatch(rawVersion)[1] + ".0.0"
}

func productToken() string {
	return "Estuary/" + productVersion()
}

var RefreshLeadTime time.Duration = 5 * time.Minute

type ObjectType string

const (
	ContactsObject  ObjectType = "contacts"
	CompaniesObject ObjectType = "companies"

	MaxResponseBytes = 10 * 1024 * 1024 // 10MiB
)

var (
	baseURL    *url.URL = MustParseURL("https://api.hubspot.com")
	tokenPath  *url.URL = MustParseURL("/oauth/v3/token")
	objectPath *url.URL = MustParseURL("/crm/objects/2026-03/")

	objectPaths map[ObjectType]*url.URL = map[ObjectType]*url.URL{
		ContactsObject:  MustParseURL("/crm/objects/2026-03/contacts"),
		CompaniesObject: MustParseURL("/crm/objects/2026-03/companies"),
	}

	// TODO: The 2026-03 URLs don't seem to work...
	propertyPaths map[ObjectType]*url.URL = map[ObjectType]*url.URL{
		ContactsObject:  MustParseURL("/crm/v3/properties/contacts"),
		CompaniesObject: MustParseURL("/crm/v3/properties/companies"),
	}
)

type APIError struct {
	StatusCode    int    `json:"omittempty"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlationId"`
	Category      string `json:"category"`
}

type TokenResponse struct {
	RefreshToken string `json:"refresh_token"`
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
}

type Property struct {
	Name        string `json:"name"`
	Label       string `json:"label"`
	Type        string `json:"type"`
	FieldType   string `json:"fieldType"`
	Description string `json:"description"`
	GroupName   string `json:"groupName"`
}

type Client struct {
	Credentials *Credentials

	httpClient *http.Client
}

func NewClient(credentials *Credentials) (*Client, error) {
	return &Client{
		Credentials: credentials,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

func (c *Client) ListObjects(ctx context.Context, objectType ObjectType) (map[string]json.RawMessage, error) {
	token, err := c.accessToken(ctx, time.Now())
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Set("limit", "10")

	path, ok := objectPaths[objectType]
	if !ok {
		return nil, fmt.Errorf("unknown object type: %v", objectType)
	}
	objectURL := baseURL.ResolveReference(path)

	var value map[string]json.RawMessage
	err = Retry[*TemporaryError](RetryDelays, func() error {
		req, err := http.NewRequestWithContext(ctx, "GET", objectURL.String(), strings.NewReader(params.Encode()))
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", productToken())
		req.Header.Set("Authorization", "Bearer "+token)

		start := time.Now()

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		defer func() {
			log.WithFields(log.Fields{
				"method":  req.Method,
				"path":    objectURL.Path,
				"status":  resp.StatusCode,
				"elapsed": time.Since(start),
			}).Info("request complete")
		}()

		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			return NewTemporaryError("unexpected response: %d", resp.StatusCode)
		}

		if resp.StatusCode != 200 {
			apiError := APIError{StatusCode: resp.StatusCode}
			_ = parseBody(resp, &apiError, MaxResponseBytes)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &value, MaxResponseBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (c *Client) GetProperties(ctx context.Context, objectType ObjectType) ([]Property, error) {
	token, err := c.accessToken(ctx, time.Now())
	if err != nil {
		return nil, err
	}

	path, ok := propertyPaths[objectType]
	if !ok {
		return nil, fmt.Errorf("unknown object type: %v", objectType)
	}
	propURL := baseURL.ResolveReference(path)

	type Response struct {
		Results []Property `json:"results"`
	}

	var value Response
	err = Retry[*TemporaryError](RetryDelays, func() error {
		req, err := http.NewRequestWithContext(ctx, "GET", propURL.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", productToken())
		req.Header.Set("Authorization", "Bearer "+token)

		start := time.Now()

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		defer func() {
			log.WithFields(log.Fields{
				"method":  req.Method,
				"path":    propURL.Path,
				"status":  resp.StatusCode,
				"elapsed": time.Since(start),
			}).Info("request complete")
		}()

		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			return NewTemporaryError("unexpected response: %d", resp.StatusCode)
		}

		if resp.StatusCode != 200 {
			apiError := APIError{StatusCode: resp.StatusCode}
			_ = parseBody(resp, &apiError, MaxResponseBytes)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &value, MaxResponseBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return value.Results, nil
}

func parseBody[T any](resp *http.Response, value *T, maxBytes int) error {
	if resp.ContentLength > int64(maxBytes) {
		return fmt.Errorf("response body too large: %d", resp.ContentLength)
	}

	body := io.LimitReader(resp.Body, int64(maxBytes))
	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("unable to read body: %w", err)
	}

	if len(data) == maxBytes {
		return fmt.Errorf("response body too large")
	}

	err = json.Unmarshal(data, &value)
	if err != nil {
		return fmt.Errorf("unable to parse json: %w", err)
	}

	return nil
}

func (c *Client) accessToken(ctx context.Context, now time.Time) (string, error) {
	switch c.Credentials.AuthType {
	case OAuth2AuthType:
		refreshTime := c.Credentials.AccessTokenExpiresAt.Add(-RefreshLeadTime)
		if now.Before(refreshTime) {
			return c.Credentials.AccessToken, nil
		}
	case ServiceKeyAuthType:
		return c.Credentials.ServiceKey, nil
	default:
		return "", fmt.Errorf("unknown auth type: %q", c.Credentials.AuthType)
	}

	params := url.Values{}
	params.Set("refresh_token", c.Credentials.RefreshToken)
	params.Set("client_id", c.Credentials.ClientID)
	params.Set("client_secret", c.Credentials.ClientSecret)
	params.Set("grant_type", "refresh_token")

	tokenURL := baseURL.ResolveReference(tokenPath)

	var tokens TokenResponse
	err := Retry[*TemporaryError](RetryDelays, func() error {
		req, err := http.NewRequestWithContext(ctx, "POST", tokenURL.String(), strings.NewReader(params.Encode()))
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", productToken())
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		start := time.Now()

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		defer func() {
			log.WithFields(log.Fields{
				"method":  req.Method,
				"path":    tokenURL.Path,
				"status":  resp.StatusCode,
				"elapsed": time.Since(start),
			}).Info("request complete")
		}()

		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			return NewTemporaryError("unexpected response: %d", resp.StatusCode)
		}

		if resp.StatusCode != 200 {
			apiError := APIError{StatusCode: resp.StatusCode}
			_ = parseBody(resp, &apiError, MaxResponseBytes)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &tokens, MaxResponseBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	c.Credentials.RefreshToken = tokens.RefreshToken
	c.Credentials.AccessToken = tokens.AccessToken
	c.Credentials.AccessTokenExpiresAt = now.Add(time.Duration(tokens.ExpiresIn) * time.Second)

	// TODO: remove
	log.WithFields(log.Fields{
		"client_secret":           c.Credentials.ClientSecret,
		"client_id":               c.Credentials.ClientID,
		"refresh_token":           c.Credentials.RefreshToken,
		"access_token":            c.Credentials.AccessToken,
		"access_token_expires_at": c.Credentials.AccessTokenExpiresAt,
	}).Info("refreshed token")

	return tokens.AccessToken, nil
}

func (c *Client) RefreshToken(ctx context.Context, now time.Time) error {
	_, err := c.accessToken(ctx, now)
	return err
}

func (c *Client) UpdateTokens(update *TokenUpdate) {
	c.Credentials.RefreshToken = update.RefreshToken
	c.Credentials.AccessToken = update.AccessToken
	c.Credentials.AccessTokenExpiresAt = update.AccessTokenExpiresAt
}

func (c *Client) TokenUpdate() *TokenUpdate {
	if c.Credentials.AuthType != OAuth2AuthType {
		return nil
	}

	return &TokenUpdate{
		RefreshToken:         c.Credentials.RefreshToken,
		AccessToken:          c.Credentials.AccessToken,
		AccessTokenExpiresAt: c.Credentials.AccessTokenExpiresAt,
	}
}

func MustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return u
}
