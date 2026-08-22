package api

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultLimit = 50
	maxLimit     = 100
)

func pageLimit(r *http.Request) int {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return defaultLimit
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultLimit
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

func encodeCursor(t time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id.String()))
}

func decodeCursor(s string) (*time.Time, uuid.UUID, error) {
	if s == "" {
		return nil, uuid.Nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, uuid.Nil, badRequest("invalid cursor")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return nil, uuid.Nil, badRequest("invalid cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, uuid.Nil, badRequest("invalid cursor")
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return nil, uuid.Nil, badRequest("invalid cursor")
	}
	return &t, id, nil
}
