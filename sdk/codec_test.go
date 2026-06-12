package conveyor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
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

func TestProtoPayloadRoundTrips(t *testing.T) {
	message := &conveyorv1.Hello{Concurrency: 8, SdkVersion: "v1.2.3"}
	payload := Proto(message)

	require.NoError(t, payload.err)
	require.Equal(t, ContentTypeProto, payload.contentType)

	task := &Task{payload: payload.data, contentType: payload.contentType}

	var decoded conveyorv1.Hello

	require.NoError(t, task.Bind(&decoded))
	require.True(t, proto.Equal(message, &decoded))
}

func TestProtoBindRejectsNonMessage(t *testing.T) {
	task := &Task{payload: nil, contentType: ContentTypeProto}

	var wrong string

	require.ErrorContains(t, task.Bind(&wrong), "bind to a proto.Message")
}
