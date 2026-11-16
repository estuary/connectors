package main

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/encoding/json"
)

type documentMetadata struct {
	Queue           string     `json:"queue"`
	ID              string     `json:"id"`
	Exchange        string     `json:"exchange"`
	RoutingKey      string     `json:"routingKey"`
	Timestamp       string     `json:"timestamp,omitempty"`
	MessageID       string     `json:"messageId,omitempty"`
	Redelivered     bool       `json:"redelivered"`
	Headers         amqp.Table `json:"headers,omitempty"`
	Offset          *int64     `json:"offset,omitempty"`
	ContentType     string     `json:"contentType,omitempty"`
	ContentEncoding string     `json:"contentEncoding,omitempty"`
	CorrelationID   string     `json:"correlationId,omitempty"`
	ReplyTo         string     `json:"replyTo,omitempty"`
	Type            string     `json:"type,omitempty"`
	AppID           string     `json:"appId,omitempty"`
	UserID          string     `json:"userId,omitempty"`
	Priority        uint8      `json:"priority"`
	DeliveryMode    uint8      `json:"deliveryMode"`
	Expiration      string     `json:"expiration,omitempty"`
}

// JSON object bodies become the document root; all other bodies are wrapped in
// body. Connector metadata always overwrites any _meta supplied by the producer.
func makeDocument(queue string, m amqp.Delivery, offset *int64) (json.RawMessage, error) {
	doc, ok := parseObjectBody(m.Body)
	if !ok {
		body := string(m.Body)
		if !utf8.Valid(m.Body) {
			body = base64.StdEncoding.EncodeToString(m.Body)
		}
		doc = map[string]any{"body": body}
	}

	m = normalizeProperties(m)
	meta := documentMetadata{
		Queue:           normalizeString(queue),
		ID:              m.MessageId,
		Exchange:        m.Exchange,
		RoutingKey:      m.RoutingKey,
		MessageID:       m.MessageId,
		Redelivered:     m.Redelivered,
		Headers:         normalizeHeaders(m.Headers),
		Offset:          offset,
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		CorrelationID:   m.CorrelationId,
		ReplyTo:         m.ReplyTo,
		Type:            m.Type,
		AppID:           m.AppId,
		UserID:          m.UserId,
		Priority:        m.Priority,
		DeliveryMode:    m.DeliveryMode,
		Expiration:      m.Expiration,
	}
	if offset != nil {
		meta.ID = meta.Queue + ":" + strconv.FormatInt(*offset, 10)
	} else if meta.ID == "" {
		meta.ID = uuid.NewString()
	}
	if timestamp := m.Timestamp.UTC(); !timestamp.IsZero() && timestamp.Year() >= 1 && timestamp.Year() <= 9999 {
		meta.Timestamp = timestamp.Format(time.RFC3339Nano)
	}
	doc["_meta"] = meta

	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("serializing document for queue %q: %w", queue, err)
	}
	return out, nil
}

func normalizeString(value string) string {
	// Escape the reserved prefix too, so literal strings cannot collide with
	// encoded bytes in message IDs, header names, or other AMQP properties.
	const prefix = "~base64:"
	if !utf8.ValidString(value) || strings.HasPrefix(value, prefix) {
		return prefix + base64.StdEncoding.EncodeToString([]byte(value))
	}
	return value
}

func normalizeProperties(m amqp.Delivery) amqp.Delivery {
	m.MessageId = normalizeString(m.MessageId)
	m.Exchange = normalizeString(m.Exchange)
	m.RoutingKey = normalizeString(m.RoutingKey)
	m.ContentType = normalizeString(m.ContentType)
	m.ContentEncoding = normalizeString(m.ContentEncoding)
	m.CorrelationId = normalizeString(m.CorrelationId)
	m.ReplyTo = normalizeString(m.ReplyTo)
	m.Type = normalizeString(m.Type)
	m.AppId = normalizeString(m.AppId)
	m.UserId = normalizeString(m.UserId)
	m.Expiration = normalizeString(m.Expiration)
	return m
}

func normalizeHeaders(headers amqp.Table) amqp.Table {
	if headers == nil {
		return nil
	}
	normalized := make(amqp.Table, len(headers))
	for key, value := range headers {
		normalized[normalizeString(key)] = normalizeHeader(value)
	}
	return normalized
}

func normalizeHeader(value any) any {
	switch v := value.(type) {
	case string:
		return normalizeString(v)
	case amqp.Table:
		return normalizeHeaders(v)
	case []any:
		normalized := make([]any, len(v))
		for i, item := range v {
			normalized[i] = normalizeHeader(item)
		}
		return normalized
	case time.Time:
		if year := v.UTC().Year(); year < 1 || year > 9999 {
			return v.Unix()
		}
		return v.UTC()
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return normalizeHeader(float64(v))
		}
	case float64:
		switch {
		case math.IsNaN(v):
			return "NaN"
		case math.IsInf(v, 1):
			return "Infinity"
		case math.IsInf(v, -1):
			return "-Infinity"
		}
	}
	return value
}

func parseObjectBody(body []byte) (map[string]any, bool) {
	if !utf8.Valid(body) {
		return nil, false
	}
	var doc map[string]any
	// Preserve large integers and reject trailing content instead of silently
	// rounding identifiers or truncating a body that isn't exactly one object.
	leftover, err := json.Parse(body, &doc, json.UseNumber)
	if err != nil || doc == nil || len(leftover) != 0 {
		return nil, false
	}
	return doc, true
}

func streamOffset(headers amqp.Table) (*int64, error) {
	value, ok := headers["x-stream-offset"]
	if !ok {
		return nil, nil
	}
	offset, ok := value.(int64)
	if !ok || offset < 0 || offset == math.MaxInt64 {
		return nil, fmt.Errorf("invalid x-stream-offset header: %v", value)
	}
	return &offset, nil
}
