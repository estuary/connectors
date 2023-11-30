package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"io"
	"net/http"
	"strings"
	"text/template"
)

type Client struct {
	cfg            config
	token          string
	httpClient     http.Client
	insertFilesTpl *template.Template
	account        string
}

const insertFilesRawTpl = "https://{{ $.Account }}.snowflakecomputing.com/v1/data/pipes/{{ $.PipeName }}/insertFiles"
const contentType = "application/json;charset=UTF-8"

func New(cfg config) (*Client, error) {
	httpClient := http.Client{}

	var key, err = cfg.privateKey()
	if err != nil {
		return nil, err
	}
	var t = jwt.New(jwt.SigningMethodRS256)
	jwtToken, err := t.SignedString(key)
	if err != nil {
		return nil, fmt.Errorf("signing key: %w", err)
	}

	var insertFilesTpl = template.Must(template.New("insertFiles").Parse(insertFilesRawTpl))

	return &Client{
		cfg:            cfg,
		httpClient:     httpClient,
		token:          jwtToken,
		insertFilesTpl: insertFilesTpl,
		account:        cfg.Account,
	}, nil
}

type insertFilesRequest struct {
	Account  string `json:"account"`
	PipeName string `json:"pipeName"`
	// Used to track the request, we generate a random uuid for this
	RequestId string `json:"requestId"`

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

func (c *Client) InsertFiles(pipeName string, files []FileRequest) (*InsertFilesResponse, error) {
	var reqBody = insertFilesRequest{
		Account:   c.account,
		PipeName:  pipeName,
		Files:     files,
		RequestId: uuid.New().String(),
	}

	var w strings.Builder
	c.insertFilesTpl.Execute(&w, reqBody)
	var url = w.String()

	reqBodyJson, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal insertFiles body: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(reqBodyJson))
	if err != nil {
		return nil, fmt.Errorf("creating insertFiles request: %w", err)
	}

	req.Header.Add("Authorization", c.token)
	req.Header.Add("Content-Type", contentType)

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
