package firebolt

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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

func New(config Config) (*Client, error) {
	httpClient := http.Client{}

	usernameAndPassword, err := json.Marshal(loginRequest{Username: config.Username, Password: config.Password})
	if err != nil {
		return nil, fmt.Errorf("creating login request failed: %w", err)
	}

	buf := strings.NewReader(string(usernameAndPassword[:]))
	resp, err := httpClient.Post(fmt.Sprintf("%s/auth/v1/login", endpoint), contentType, buf)

	if err != nil {
		return nil, fmt.Errorf("login request to get access token failed: %w", err)
	}

	respBuf := new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()

	if err != nil {
		return nil, fmt.Errorf("reading response body of login request to get access token failed: %w", err)
	}

	var response loginResponse
	err = json.Unmarshal([]byte(respBuf.String()), &response)

	if err != nil {
		return nil, fmt.Errorf("parsing response body of login request to get access token failed: %s, %w", respBuf, err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("login request to get access token failed: %w", err)
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

func (c *Client) Query(query io.Reader) (*http.Response, error) {
	url := fmt.Sprintf("%s/?database=%s", c.config.EngineURL, c.config.Database)
	req, err := http.NewRequest("POST", url, query)
	if err != nil {
		return nil, fmt.Errorf("creating query request failed: %w", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("%s %s", c.tokenType, c.accessToken))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query request failed: %w", err)
	}

	return resp, nil
}
