package main

import (
	"bytes"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/template"
	"time"
)

type PipeClient struct {
	cfg            *config
	base           string
	token          string
	httpClient     http.Client
	insertFilesTpl *template.Template
	account        string
}

func publicKeyFingerprint(publicKey *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("marshalling public key: %w", err)
	}
	var hash = sha256.Sum256(der)
	return fmt.Sprintf("SHA256:%s", base64.StdEncoding.EncodeToString(hash[:])), nil
}

func NewPipeClient(cfg *config, tenant string) (*PipeClient, error) {
	httpClient := http.Client{}

	var key, err = cfg.privateKey()
	if err != nil {
		return nil, err
	}

	var account = strings.ToUpper(cfg.Account)
	var user = strings.ToUpper(cfg.User)
	var qualifiedUser = fmt.Sprintf("%s.%s", account, user)

	fingerprint, err := publicKeyFingerprint(key.Public().(*rsa.PublicKey))
	if err != nil {
		return nil, err
	}

	log.WithField("fingerprint", fingerprint).Warn("pipe client")

	var claims = &jwt.RegisteredClaims{
		IssuedAt: jwt.NewNumericDate(time.Now()),
		// TODO: automatically refresh the JWT token
		// JWT tokens for Snowflake can live up to an hour
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(59 * time.Minute)),
		Issuer:    fmt.Sprintf("%s.%s", qualifiedUser, fingerprint),
		Subject:   qualifiedUser,
	}

	var t = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	jwtToken, err := t.SignedString(key)
	if err != nil {
		return nil, fmt.Errorf("signing key: %w", err)
	}
	log.WithField("token", jwtToken).Warn("pipe client")

	var insertFilesTpl = template.Must(template.New("insertFiles").Parse(insertFilesRawTpl))

	var dsn = cfg.ToURI(tenant)
	dsnURL, err := url.Parse(fmt.Sprintf("https://%s", dsn))
	if err != nil {
		return nil, fmt.Errorf("parsing snowflake dsn: %w", err)
	}
	log.WithField("dsn", dsn).WithField("dsnURL", dsnURL).Warn("pipe client")

	return &PipeClient{
		cfg:            cfg,
		base:           dsnURL.Hostname(),
		httpClient:     httpClient,
		token:          jwtToken,
		insertFilesTpl: insertFilesTpl,
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
	Status    string `json:"status"`
	RequestId string `json:"requestId"`
}

const insertFilesRawTpl = "https://{{ $.Base }}/v1/data/pipes/{{ $.PipeName }}/insertFiles?requestId={{ $.RequestId }}"
const contentType = "application/json;charset=UTF-8"

// pipeName must be a fully-qualified name, e.g.: database.schema.pipe
func (c *PipeClient) InsertFiles(pipeName string, files []FileRequest) (*InsertFilesResponse, error) {
	var urlTemplate = insertFilesURLTemplate{
		Base:      c.base,
		PipeName:  strings.ToLower(pipeName),
		RequestId: uuid.New().String(),
	}

	var reqBody = insertFilesRequest{
		Files: files,
	}

	var w strings.Builder
	c.insertFilesTpl.Execute(&w, urlTemplate)
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

	log.WithFields(log.Fields{
		"url":     url,
		"body":    string(reqBodyJson),
		"auth":    c.token,
		"headers": req.Header,
	}).Warn("pipe client")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("insertFiles request: %w", err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response of insertFiles: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("response error code %d, %s", resp.StatusCode, respBuf)
	}

	var response InsertFilesResponse
	err = json.Unmarshal([]byte(respBuf.String()), &response)
	if err != nil {
		return nil, fmt.Errorf("parsing response of insertFiles failed: %w", err)
	}

	return &response, nil
}
