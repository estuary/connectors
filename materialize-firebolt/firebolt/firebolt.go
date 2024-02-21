package firebolt

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type Config struct {
	EngineURL string
	Database  string
	Username  string
	Password  string
}

type Client struct {
	config         Config
	httpClient     http.Client
	tokenAcquired  time.Time
	tokenExpiresIn time.Time
	accessToken    string
	refreshToken   string
	tokenType      string
}

type loginRequest struct {
	Username string
	Password string
}

type loginResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

const endpoint = "https://api.app.firebolt.io"
const contentType = "application/json;charset=UTF-8"

// How many minutes before token expiry should
// we attempt to refresh tokens?
const tokenRefreshMinutes = 30

func New(config Config) (*Client, error) {
	httpClient := http.Client{}

	var usernameAndPassword, err = json.Marshal(loginRequest{Username: config.Username, Password: config.Password})
	if err != nil {
		return nil, fmt.Errorf("creating login request failed: %w", err)
	}

	var buf = strings.NewReader(string(usernameAndPassword[:]))
	resp, err := httpClient.Post(fmt.Sprintf("%s/auth/v1/login", endpoint), contentType, buf)

	if err != nil {
		return nil, fmt.Errorf("login request to get access token failed: %w", err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()

	if err != nil {
		return nil, fmt.Errorf("reading response body of login request to get access token failed: %w", err)
	}

	var response loginResponse
	var respStr = respBuf.String()
	err = json.Unmarshal([]byte(respStr), &response)

	if err != nil {
		return nil, fmt.Errorf("parsing response body of login request to get access token failed: %s, %w", respBuf, err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("login request to get access token failed (%d): %s", resp.StatusCode, respStr)
	}

	expiryDuration, err := time.ParseDuration(fmt.Sprintf("%ds", response.ExpiresIn))

	if err != nil {
		return nil, fmt.Errorf("parsing expiry duration failed: %w", err)
	}

	return &Client{
		config:         config,
		httpClient:     httpClient,
		accessToken:    response.AccessToken,
		refreshToken:   response.RefreshToken,
		tokenAcquired:  time.Now(),
		tokenExpiresIn: time.Now().Add(expiryDuration),
		tokenType:      response.TokenType,
	}, nil
}

type QueryMeta struct {
	Name string
	Type string `json:"type"`
}

type QueryStatistics struct {
	Elapsed   float64
	RowsRead  int `json:"rows_read"`
	BytesRead int `json:"bytes_read"`
}

type QueryResponse struct {
	Meta       []QueryMeta
	Data       []map[string]interface{}
	Rows       int
	Statistics QueryStatistics
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type refreshResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
	TokenType   string `json:"token_type"`
}

func (c *Client) RefreshToken() error {
	var refreshRequest, err = json.Marshal(refreshRequest{RefreshToken: c.refreshToken})
	if err != nil {
		return fmt.Errorf("creating refresh request failed: %w", err)
	}

	var buf = strings.NewReader(string(refreshRequest[:]))
	resp, err := c.httpClient.Post(fmt.Sprintf("%s/auth/v1/refresh", endpoint), contentType, buf)

	if err != nil {
		return fmt.Errorf("refresh request to refresh access token failed: %w", err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()

	if err != nil {
		return fmt.Errorf("reading response body of refresh request to refresh access token failed: %w", err)
	}

	var response refreshResponse
	var respStr = respBuf.String()
	err = json.Unmarshal([]byte(respStr), &response)

	if err != nil {
		return fmt.Errorf("parsing response body of login request to get access token failed: %s, %w", respBuf, err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("refresh request to get access token failed (%d): %s", resp.StatusCode, respStr)
	}

	expiryDuration, err := time.ParseDuration(fmt.Sprintf("%ds", response.ExpiresIn))

	if err != nil {
		return fmt.Errorf("parsing expiry duration failed: %w", err)
	}
	c.accessToken = response.AccessToken
	c.tokenAcquired = time.Now()
	c.tokenExpiresIn = time.Now().Add(expiryDuration)
	c.tokenType = response.TokenType

	return nil
}

func (c *Client) Query(query string) (*QueryResponse, error) {
	if time.Until(c.tokenExpiresIn).Minutes() < tokenRefreshMinutes {
		var err = c.RefreshToken()
		if err != nil {
			return nil, fmt.Errorf("refreshing token failed: %w", err)
		}
	}

	var url = fmt.Sprintf("https://%s/?database=%s", c.config.EngineURL, c.config.Database)
	log.WithField("url", url).Debug("query request")
	var req, err = http.NewRequest("POST", url, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("creating query request failed: %w", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("%s %s", c.tokenType, c.accessToken))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query request failed: %w", err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response of query failed: %w", err)
	}

	if resp.StatusCode == 503 {
		return nil, fmt.Errorf("Received 503 error when trying to connect to the engine. Please make sure your engine `%s` is up and running. It is advised that you set your engine to \"Always On\" in Firebolt to avoid auto-stopping of your engine.", c.config.EngineURL)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("response error code %d, %s", resp.StatusCode, respBuf.String())
	}

	var queryResponse QueryResponse
	err = json.Unmarshal([]byte(respBuf.String()), &queryResponse)
	if err != nil {
		return nil, fmt.Errorf("parsing response of query failed: %w", err)
	}

	return &queryResponse, nil
}
