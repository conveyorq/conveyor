package conveyor

import (
	"errors"
	"fmt"
	"testing"
)

func TestSkipRetryDetection(t *testing.T) {
	cause := errors.New("payload decode failed")
	err := SkipRetry(cause)

	if !IsSkipRetry(err) {
		t.Fatal("IsSkipRetry must detect a SkipRetry error")
	}

	if !errors.Is(err, cause) {
		t.Fatal("SkipRetry must unwrap to its cause")
	}
}

func TestSkipRetrySurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("handler: %w", SkipRetry(errors.New("bad input")))

	if !IsSkipRetry(err) {
		t.Fatal("IsSkipRetry must see through fmt.Errorf wrapping")
	}
}

func TestPlainErrorIsNotSkipRetry(t *testing.T) {
	if IsSkipRetry(errors.New("transient")) {
		t.Fatal("plain errors must not be skip-retry")
	}

	if IsSkipRetry(nil) {
		t.Fatal("nil must not be skip-retry")
	}
}
