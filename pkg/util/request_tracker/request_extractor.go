package request_tracker

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cortexproject/cortex/pkg/util/requestmeta"
	"github.com/cortexproject/cortex/pkg/util/users"
)

type Extractor interface {
	Extract(r *http.Request) []byte
}

type DefaultExtractor struct{}

type ApiExtractor struct{}

type InstantQueryExtractor struct{}

type RangedQueryExtractor struct{}

func generateCommonMap(r *http.Request) map[string]interface{} {
	ctx := r.Context()
	entryMap := make(map[string]interface{})
	entryMap["timestamp-sec"] = time.Now().Unix()
	entryMap["Path"] = r.URL.Path
	entryMap["Method"] = r.Method
	entryMap["TenantID"], _ = users.TenantID(ctx)
	entryMap["RequestID"] = requestmeta.RequestIdFromContext(ctx)
	entryMap["UserAgent"] = r.Header.Get("User-Agent")
	entryMap["DashboardUID"] = r.Header.Get("X-Dashboard-UID")
	entryMap["PanelId"] = r.Header.Get("X-Panel-Id")

	return entryMap
}

func (e *DefaultExtractor) Extract(r *http.Request) []byte {
	entryMap := generateCommonMap(r)

	return generateJSONEntry(entryMap)
}

func (e *ApiExtractor) Extract(r *http.Request) []byte {
	entryMap := generateCommonMap(r)
	entryMap["limit"] = r.URL.Query().Get("limit")
	entryMap["start"] = r.URL.Query().Get("start")
	entryMap["end"] = r.URL.Query().Get("end")

	matches := r.URL.Query()["match[]"]
	entryMap["number-of-matches"] = len(matches)

	minEntryJSON := generateJSONEntry(entryMap)

	matchesStr := strings.Join(matches, ",")
	matchesStr = trimStringByBytes(matchesStr, maxEntrySize-(len(minEntryJSON)+1))
	entryMap["matches"] = matchesStr

	return generateJSONEntry(entryMap)
}

func (e *InstantQueryExtractor) Extract(r *http.Request) []byte {
	entryMap := generateCommonMap(r)
	entryMap["time"] = r.URL.Query().Get("time")

	minEntryJSON := generateJSONEntry(entryMap)
	query := r.URL.Query().Get("query")
	query = trimStringByBytes(query, maxEntrySize-(len(minEntryJSON)+1))
	entryMap["query"] = query

	return generateJSONEntry(entryMap)
}

func (e *RangedQueryExtractor) Extract(r *http.Request) []byte {
	entryMap := generateCommonMap(r)
	entryMap["start"] = r.URL.Query().Get("start")
	entryMap["end"] = r.URL.Query().Get("end")
	entryMap["step"] = r.URL.Query().Get("step")

	minEntryJSON := generateJSONEntry(entryMap)
	query := r.URL.Query().Get("query")
	query = trimStringByBytes(query, maxEntrySize-(len(minEntryJSON)+1))
	entryMap["query"] = query

	return generateJSONEntry(entryMap)
}

func generateJSONEntry(entryMap map[string]interface{}) []byte {
	jsonEntry, err := json.Marshal(entryMap)
	if err != nil {
		return []byte{}
	}

	return jsonEntry
}

func trimStringByBytes(str string, size int) string {
	bytesStr := []byte(str)

	trimIndex := len(bytesStr)
	if size < len(bytesStr) {
		for !utf8.RuneStart(bytesStr[size]) {
			size--
		}
		trimIndex = size
	}

	return string(bytesStr[:trimIndex])
}
