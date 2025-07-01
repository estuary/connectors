package main

import (
	"bytes"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type pipeParts struct {
	Catalog   string
	Schema    string
	Binding   string
	KeyBegin  string
	Version   string
	TableName string
}

func (parts *pipeParts) toPipeName() string {
	return strings.ToUpper(fmt.Sprintf("FLOW_PIPE_%s_%s_%s_%s", parts.Binding, parts.KeyBegin, parts.Version, parts.TableName))
}

func (parts *pipeParts) toQualifiedName() string {
	return strings.ToUpper(fmt.Sprintf("%s.%s.%s", parts.Catalog, parts.Schema, parts.toPipeName()))
}

// Deconstruct a pipeName into its constituent parts
func pipeNameToParts(pipeName string) pipeParts {
	pipeNameSplit := strings.Split(pipeName, ".")
	nameParts := strings.Split(strings.TrimPrefix(pipeNameSplit[2], "FLOW_PIPE_"), "_")
	tableName := strings.Join(nameParts[3:], "_")
	return pipeParts{
		Catalog:   pipeNameSplit[0],
		Schema:    pipeNameSplit[1],
		Binding:   nameParts[0],
		KeyBegin:  nameParts[1],
		Version:   nameParts[2],
		TableName: tableName,
	}
}

var pipeSanitizeRegex = regexp.MustCompile("(?i)[^a-z0-9_]")

// sanitizeAndAppendHash adapts an input into a reasonably human-readable
// representation, sanitizing problematic characters and including a hash of the
// "original" value to guarantee a unique (with respect to the input) and
// deterministic output.
func sanitizeAndAppendHash(tableName string) string {
	sanitizedTable := pipeSanitizeRegex.ReplaceAllString(tableName, "_")

	limited := sanitizedTable

	if len(sanitizedTable) > 32 {
		// Limit the length of the "human readable" part of the table name to
		// something reasonable, but make sure only the tableName portion is cut
		limited = sanitizedTable[:32]
	}

	return fmt.Sprintf("%s_%016X", limited, xxhash.Sum64String(tableName))
}

type PipeClient struct {
	cfg             *config
	base            string
	accountName     string
	token           string
	expiry          time.Time
	httpClient      http.Client
	insertFilesTpl  *template.Template
	insertReportTpl *template.Template
}

func publicKeyFingerprint(publicKey *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("marshalling public key: %w", err)
	}
	var hash = sha256.Sum256(der)
	return fmt.Sprintf("SHA256:%s", base64.StdEncoding.EncodeToString(hash[:])), nil
}

// See https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-key-pair-authentication
// for details on how the JWT token is constructed
func generateJWTToken(key *rsa.PrivateKey, user string, accountName string) (string, time.Time, error) {
	fingerprint, err := publicKeyFingerprint(key.Public().(*rsa.PublicKey))
	if err != nil {
		return "", time.UnixMilli(0), err
	}

	var qualifiedUser = fmt.Sprintf("%s.%s", strings.ToUpper(accountName), strings.ToUpper(user))

	// JWT tokens for Snowflake can live up to an hour
	var expiry = time.Now().Add(59 * time.Minute)

	var claims = &jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(expiry),
		Issuer:    fmt.Sprintf("%s.%s", qualifiedUser, fingerprint),
		Subject:   qualifiedUser,
	}

	var t = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	jwtToken, err := t.SignedString(key)
	if err != nil {
		return "", time.UnixMilli(0), fmt.Errorf("signing key: %w", err)
	}

	return jwtToken, expiry, nil
}

func NewPipeClient(cfg *config, accountName string, tenant string) (*PipeClient, error) {
	httpClient := http.Client{}

	var dsn, err = cfg.toURI(tenant)
	if err != nil {
		return nil, fmt.Errorf("building snowflake dsn: %w", err)
	}

	dsnURL, err := url.Parse(fmt.Sprintf("https://%s", dsn))
	if err != nil {
		return nil, fmt.Errorf("parsing snowflake dsn: %w", err)
	}

	key, err := cfg.Credentials.ParsePrivateKey()
	if err != nil {
		return nil, err
	}

	jwtToken, expiry, err := generateJWTToken(key, cfg.Credentials.User, accountName)
	if err != nil {
		return nil, fmt.Errorf("creating jwt token: %w", err)
	}

	var insertFilesTpl = template.Must(template.New("insertFiles").Parse(insertFilesRawTpl))
	var insertReportTpl = template.Must(template.New("insertReport").Parse(insertReportRawTpl))

	return &PipeClient{
		cfg:             cfg,
		accountName:     accountName,
		base:            dsnURL.Hostname(),
		httpClient:      httpClient,
		token:           jwtToken,
		expiry:          expiry,
		insertFilesTpl:  insertFilesTpl,
		insertReportTpl: insertReportTpl,
	}, nil
}

type insertFilesURLTemplate struct {
	Base     string
	PipeName string
	// Used to track the request, we generate a random uuid for this
	RequestId string
}

type insertFilesRequest struct {
	Files []FileRequest `json:"files"`
}

type FileRequest struct {
	Path string `json:"path"`
	Size int    `json:"size"`
}

type InsertFilesResponse struct {
	Status    string `json:"responseCode"`
	RequestId string `json:"requestId"`
}

type InsertFilesError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Success bool   `json:"success"`
}

func (e InsertFilesError) Error() string {
	return fmt.Sprintf("(%s) %s", e.Code, e.Message)
}

const insertFilesRawTpl = "https://{{ $.Base }}/v1/data/pipes/{{ $.PipeName }}/insertFiles?requestId={{ $.RequestId }}"
const contentType = "application/json;charset=UTF-8"
const userAgent = "Estuary Technologies Flow"

func (c *PipeClient) refreshJWT() error {
	if time.Until(c.expiry).Minutes() < 5 {
		var key, err = c.cfg.Credentials.ParsePrivateKey()
		if err != nil {
			return err
		}
		jwtToken, expiry, err := generateJWTToken(key, c.cfg.Credentials.User, c.accountName)
		if err != nil {
			return fmt.Errorf("recreating jwt token: %w", err)
		}

		c.token = jwtToken
		c.expiry = expiry
	}

	return nil
}

// pipeName must be a fully-qualified name, e.g.: database.schema.pipe
func (c *PipeClient) InsertFiles(pipeName string, files []FileRequest) (*InsertFilesResponse, error) {
	if err := c.refreshJWT(); err != nil {
		return nil, err
	}

	var requestId = uuid.New().String()
	var urlTemplate = insertFilesURLTemplate{
		Base:      c.base,
		PipeName:  strings.ToLower(pipeName),
		RequestId: requestId,
	}

	var reqBody = insertFilesRequest{
		Files: files,
	}

	var w strings.Builder
	if err := c.insertFilesTpl.Execute(&w, urlTemplate); err != nil {
		return nil, fmt.Errorf("insertFiles template: %w", err)
	}
	var url = w.String()

	reqBodyJson, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal insertFiles body: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(reqBodyJson))
	if err != nil {
		return nil, fmt.Errorf("creating insertFiles request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("BEARER %s", c.token))
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("insertFiles request (%s): %w", requestId, err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response of insertFiles (%s): %w", requestId, err)
	}

	log.WithFields(log.Fields{
		"url":  url,
		"body": string(reqBodyJson),
		"resp": respBuf.String(),
	}).Debug("pipe client")

	if resp.StatusCode >= 400 {
		var errResponse InsertFilesError
		if err := json.Unmarshal([]byte(respBuf.String()), &errResponse); err != nil {
			return nil, fmt.Errorf("response error code (%s): %d %s", requestId, resp.StatusCode, respBuf.String())
		} else {
			return nil, errResponse
		}
	}

	var response InsertFilesResponse
	if err := json.Unmarshal([]byte(respBuf.String()), &response); err != nil {
		return nil, fmt.Errorf("parsing response of insertFiles failed (%s): %w", requestId, err)
	}

	if response.Status != "SUCCESS" {
		return nil, fmt.Errorf("response status %q, %s", response.Status, respBuf.String())
	}

	return &response, nil
}

const insertReportRawTpl = "https://{{ $.Base }}/v1/data/pipes/{{ $.PipeName }}/insertReport?requestId={{ $.RequestId }}"

type insertReportURLTemplate struct {
	Base     string
	PipeName string
	// Used to track the request, we generate a random uuid for this
	RequestId string

	BeginMark string
}

type fileReport struct {
	Path                   string `json:"path"`
	StageLocation          string `json:"stageLocation"`
	FileSize               int    `json:"fileSize"`
	TimeReceived           string `json:"timeReceived"`
	LastInsertTime         string `json:"lastInsertTime"`
	RowsInserted           int    `json:"rowsInserted"`
	RowsParsed             int    `json:"rowsParsed"`
	ErrorsSeen             int    `json:"errorsSeen"`
	ErrorLimit             int    `json:"errorLimit"`
	FirstError             string `json:"firstError"`
	FirstErrorLineNum      int    `json:"firstErrorLineNum"`
	FirstErrorCharacterPos int    `json:"firstErrorCharacterPos"`
	FirstErrorColumnName   int    `json:"firstErrorColumnName"`
	SystemError            string `json:"systemError"`
	Complete               bool   `json:"complete"`
	Status                 string `json:"status"`
}

type InsertReportResponse struct {
	NextBeginMark string       `json:"nextBeginMark"`
	Files         []fileReport `json:"files"`
}

func (c *PipeClient) InsertReport(pipeName string) (*InsertReportResponse, error) {
	if err := c.refreshJWT(); err != nil {
		return nil, err
	}

	var requestId = uuid.New().String()
	var urlTemplate = insertReportURLTemplate{
		Base:      c.base,
		PipeName:  strings.ToLower(pipeName),
		RequestId: requestId,
	}

	var w strings.Builder
	if err := c.insertReportTpl.Execute(&w, urlTemplate); err != nil {
		panic(err)
	}
	var url = w.String()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating insertReport request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("BEARER %s", c.token))
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("insertReport request (%s): %w", requestId, err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response of insertReport (%s): %w", requestId, err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("response error code (%s): %d %s", requestId, resp.StatusCode, respBuf)
	}

	var response InsertReportResponse
	if err := json.Unmarshal([]byte(respBuf.String()), &response); err != nil {
		return nil, fmt.Errorf("parsing response of insertReport failed (%s): %w", requestId, err)
	}

	log.WithFields(log.Fields{
		"url":      url,
		"headers":  req.Header,
		"response": response,
	}).Debug("insertReport")

	return &response, nil
}
