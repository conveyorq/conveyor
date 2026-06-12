package conveyor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONPayloadEncodes(t *testing.T) {
	payload := JSON(map[string]int{"user_id": 42})

	require.NoError(t, payload.err)
	require.JSONEq(t, `{"user_id":42}`, string(payload.data))
	require.Equal(t, ContentTypeJSON, payload.contentType)
}

func TestJSONPayloadCarriesEncodingError(t *testing.T) {
	payload := JSON(make(chan int))

	require.Error(t, payload.err)
	require.ErrorContains(t, payload.err, "encoding JSON payload")
}

func TestBytesPayloadIsVerbatim(t *testing.T) {
	raw := []byte{0x01, 0x02, 0x03}
	payload := Bytes(raw)

	require.NoError(t, payload.err)
	require.Equal(t, raw, payload.data)
	require.Equal(t, ContentTypeBytes, payload.contentType)
}
