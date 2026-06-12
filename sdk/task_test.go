package conveyor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTaskCarriesTypeAndPayload(t *testing.T) {
	task := NewTask("email:welcome", JSON(map[string]int{"user_id": 42}))

	require.Equal(t, "email:welcome", task.Type())
	require.Equal(t, ContentTypeJSON, task.ContentType())
	require.JSONEq(t, `{"user_id":42}`, string(task.Payload()))
	require.Empty(t, task.ID())
	require.Empty(t, task.Queue())
	require.Zero(t, task.Retried())
	require.Zero(t, task.MaxRetry())
	require.Nil(t, task.Metadata())
}

func TestBindJSON(t *testing.T) {
	task := &Task{payload: []byte(`{"user_id":42}`), contentType: ContentTypeJSON}

	var decoded struct {
		UserID int `json:"user_id"`
	}

	require.NoError(t, task.Bind(&decoded))
	require.Equal(t, 42, decoded.UserID)
}

func TestBindJSONFailure(t *testing.T) {
	task := &Task{payload: []byte(`not json`), contentType: ContentTypeJSON}

	var decoded map[string]any

	require.ErrorContains(t, task.Bind(&decoded), "binding JSON payload")
}

func TestBindBytes(t *testing.T) {
	raw := []byte{0xCA, 0xFE}
	task := &Task{payload: raw, contentType: ContentTypeBytes}

	var decoded []byte

	require.NoError(t, task.Bind(&decoded))
	require.Equal(t, raw, decoded)
}

func TestBindBytesNeedsByteSlicePointer(t *testing.T) {
	task := &Task{payload: []byte{0x01}, contentType: ContentTypeBytes}

	var wrong string

	require.ErrorContains(t, task.Bind(&wrong), "bind to *[]byte")
}

func TestBindUnknownContentType(t *testing.T) {
	task := &Task{payload: []byte("x"), contentType: "application/xml"}

	var decoded any

	require.ErrorContains(t, task.Bind(&decoded), "no codec for content type")
}
