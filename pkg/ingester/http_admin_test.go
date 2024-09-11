package ingester

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserStatsPageRendered(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/ingester/all_user_stats", nil)
	res := httptest.NewRecorder()
	userStats := []UserIDStats{
		{
			UserID: "123",
			UserStats: UserStats{
				IngestionRate:     11.11,
				NumSeries:         2222,
				APIIngestionRate:  33.33,
				RuleIngestionRate: 44.44,
				ActiveSeries:      5555,
				LoadedBlocks:      6666,
			},
		},
	}
	AllUserStatsRender(res, req, userStats, 3)
	assert.Equal(t, http.StatusOK, res.Code)
	body := res.Body.String()
	assert.Regexp(t, "<td.+123.+/td>", body)
	assert.Regexp(t, "<td.+11.11.+/td>", body)
	assert.Regexp(t, "<td.+2222.+/td>", body)
	assert.Regexp(t, "<td.+33.33.+/td>", body)
	assert.Regexp(t, "<td.+44.44.+/td>", body)
	assert.Regexp(t, "<td.+5555.+/td>", body)
	assert.Regexp(t, "<td.+6666.+/td>", body)
}
