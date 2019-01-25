package mapper

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/common/model"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"

	"github.com/cortexproject/cortex/pkg/chunk/testutils"
)

var mapper = &Mapper{
	Users: map[string]string{"1": "2"},
}

func Test_Mapper_MapChunks(t *testing.T) {
	now := model.Now()
	_, input, _ := testutils.CreateChunks(1, 1, testutils.UserOpt("1"), testutils.From(now))
	_, want, _ := testutils.CreateChunks(1, 1, testutils.UserOpt("2"), testutils.From(now))

	got, _ := mapper.MapChunks(input)
	for i := range want {
		err := compareChunks(got[i], want[i])
		require.NoError(t, err)
	}
}

func compareChunks(got, want chunk.Chunk) error {
	if got.Fingerprint != want.Fingerprint {
		return fmt.Errorf("fingerprint does not match, %v != %v", got.Fingerprint, want.Fingerprint)
	}

	if got.UserID != want.UserID {
		return fmt.Errorf("userID does not match, %v != %v", got.UserID, want.UserID)
	}

	if got.From != want.From {
		return fmt.Errorf("from does not match, %v != %v", got.From, want.From)
	}

	if got.Through != want.Through {
		return fmt.Errorf("through does not match, %v != %v", got.Through, want.Through)
	}

	if !reflect.DeepEqual(got.Metric, want.Metric) {
		return fmt.Errorf("metric does not match, %v != %v", got.Metric, want.Metric)
	}

	if got.Encoding != want.Encoding {
		return fmt.Errorf("encoding does not match, %v != %v", got.Encoding, want.Encoding)
	}

	if !reflect.DeepEqual(got.Data, want.Data) {
		return fmt.Errorf("data does not match, %v != %v", got.Data, want.Data)
	}

	return nil
}
