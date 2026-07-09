// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/conveyorq/conveyor/internal/clock"
)

// Delivery-signing headers. The signature binds the timestamp and the exact
// request body under the registration's secret, so an endpoint can verify
// both origin and integrity; the timestamp bounds replay.
const (
	// TimestampHeader carries the unix-seconds send time.
	TimestampHeader = "X-Conveyor-Timestamp"
	// SignatureHeader carries the versioned body signature.
	SignatureHeader = "X-Conveyor-Signature"
	// signatureVersionPrefix versions the signing scheme.
	signatureVersionPrefix = "v1="
)

// HMACSigner signs deliveries with HMAC-SHA256 over "{timestamp}.{body}",
// keyed by the registration's newest secret. Receivers verifying against
// any of their configured secrets keeps a rotation in progress valid.
type HMACSigner struct {
	// secret is the signing key.
	secret string
	// timeSource reads the send time.
	timeSource clock.Clock
}

// enforce interface compliance at compile time.
var _ Signer = (*HMACSigner)(nil)

// NewHMACSigner builds a signer keyed by the given secret, reading send
// times from the given clock.
func NewHMACSigner(secret string, timeSource clock.Clock) *HMACSigner {
	return &HMACSigner{secret: secret, timeSource: timeSource}
}

// Sign implements Signer: it stamps the timestamp and signature headers
// derived from the request body.
func (s *HMACSigner) Sign(header http.Header, body []byte) {
	timestamp := strconv.FormatInt(s.timeSource.Now().Unix(), 10)

	header.Set(TimestampHeader, timestamp)
	header.Set(SignatureHeader, signatureVersionPrefix+SignBody(s.secret, timestamp, body))
}

// SignBody computes the hex HMAC-SHA256 of "{timestamp}.{body}" under
// secret. It is exported so receivers (and tests) can compute the expected
// signature with the same code the sender uses.
func SignBody(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)

	return hex.EncodeToString(mac.Sum(nil))
}
