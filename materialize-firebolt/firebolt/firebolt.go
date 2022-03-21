package firebolt

import (
	"encoding/json"
	"fmt"
	//log "github.com/sirupsen/logrus"
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

// TODO: refresh token when it expires
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

func (c *Client) Query(query string) (*QueryResponse, error) {
	url := fmt.Sprintf("https://%s/?database=%s", c.config.EngineURL, c.config.Database)
	req, err := http.NewRequest("POST", url, strings.NewReader(query))
	if err != nil {
		return nil, fmt.Errorf("creating query request failed: %w", err)
	}
	req.Header.Add("Authorization", fmt.Sprintf("%s %s", c.tokenType, c.accessToken))
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query request failed: %w", err)
	}

	respBuf := new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response of query failed: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("response error code %d, %s", resp.StatusCode, respBuf)
	}

	var queryResponse QueryResponse
	err = json.Unmarshal([]byte(respBuf.String()), &queryResponse)
	if err != nil {
		return nil, fmt.Errorf("parsing response of query failed: %w", err)
	}

	return &queryResponse, nil
}

/*func (c *Client) CreateExternalTable(tableName string, schema string, url string, awsKeyId string, awsSecretKey string) (*QueryResponse, error) {
	query := fmt.Sprintf(
		"CREATE EXTERNAL TABLE IF NOT EXISTS %s (%s) TYPE=(JSON) URL='%s' OBJECT_PATTERN='*.json' CREDENTIALS = (AWS_KEY_ID = '%s' AWS_SECRET_KEY = '%s');",
		tableName,
		schema,
		url,
		awsKeyId,
		awsSecretKey,
	)
	log.Info("Create Exernal Table Query ", query)
	return c.Query(strings.NewReader(query))
}

func (c *Client) CreateTable(tableType string, tableName string, schema string, primaryIndex string) (*QueryResponse, error) {
	query := fmt.Sprintf(
		"CREATE %s TABLE IF NOT EXISTS %s (%s,source_file_name TEXT) PRIMARY INDEX %s;",
		strings.ToUpper(tableType),
		tableName,
		schema,
		primaryIndex,
	)
	log.Info("Create Table Query ", query)
	return c.Query(strings.NewReader(query))
}

func (c *Client) DropTable(tableName string) (*QueryResponse, error) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName)
	log.Info("Drop Table Query ", query)
	return c.Query(strings.NewReader(query))
}

func (c *Client) InsertRow(tableName string, columns []string, values []string) (*QueryResponse, error) {
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s);",
		tableName,
		strings.Join(columns, ","),
		strings.Join(values, ","),
	)
	log.Info("Insert Query ", query)
	return c.Query(strings.NewReader(query))
}

func (c *Client) InsertRows(tableName string, columns []string, values [][]string) (*QueryResponse, error) {
	var valuesQueries []string
	for _, v := range values {
		valuesQueries = append(valuesQueries, strings.Join(v, ","))
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s;",
		tableName,
		strings.Join(columns, ","),
		strings.Join(valuesQueries, ","),
	)
	log.Info("Insert Query ", query)
	return c.Query(strings.NewReader(query))
}

func (c *Client) InsertFromExternal(tableName string, sourceFileNames []string) (*QueryResponse, error) {
	names := "'" + strings.Join(sourceFileNames, "','") + "'"
	query := fmt.Sprintf("INSERT INTO %s SELECT *, source_file_name FROM %s_external WHERE source_file_name IN (%s);", tableName, tableName, names)

	log.Info("Insert From External Query ", query)
	return c.Query(strings.NewReader(query))
}*/
