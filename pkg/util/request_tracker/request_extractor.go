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
	matchesStr := strings.Join(matches, ",")

	return generateJSONEntryWithTruncatedField(entryMap, "matches", matchesStr)
}

func (e *InstantQueryExtractor) Extract(r *http.Request) []byte {
	entryMap := generateCommonMap(r)
	entryMap["time"] = r.URL.Query().Get("time")
	return generateJSONEntryWithTruncatedField(entryMap, "query", r.URL.Query().Get("query"))
}

func (e *RangedQueryExtractor) Extract(r *http.Request) []byte {
	entryMap := generateCommonMap(r)
	entryMap["start"] = r.URL.Query().Get("start")
	entryMap["end"] = r.URL.Query().Get("end")
	entryMap["step"] = r.URL.Query().Get("step")
	return generateJSONEntryWithTruncatedField(entryMap, "query", r.URL.Query().Get("query"))
}

func generateJSONEntry(entryMap map[string]interface{}) []byte {
	jsonEntry, err := json.Marshal(entryMap)
	if err != nil {
		return []byte{}
	}

	return jsonEntry
}

func generateJSONEntryWithTruncatedField(entryMap map[string]interface{}, fieldName, fieldValue string) []byte {
	entryMap[fieldName] = ""
	minEntryJSON := generateJSONEntry(entryMap)
	entryMap[fieldName] = trimForJsonMarshal(fieldValue, maxEntrySize-(len(minEntryJSON)+1))
	return generateJSONEntry(entryMap)
}

func trimStringByBytes(bytesStr []byte, size int) string {
	trimIndex := len(bytesStr)
	if size < len(bytesStr) {
		for !utf8.RuneStart(bytesStr[size]) {
			size--
		}
		trimIndex = size
	}

	return string(bytesStr[:trimIndex])
}

func trimForJsonMarshal(field string, size int) string {
	fieldValueEncoded, err := json.Marshal(field)
	if err != nil {
		return ""
	}
	fieldValueEncoded = fieldValueEncoded[1 : len(fieldValueEncoded)-1]
	fieldValueEncodedTrimmed := trimStringByBytes(fieldValueEncoded, size)
	fieldValueEncodedTrimmed = "\"" + removeHalfCutEscapeChar(fieldValueEncodedTrimmed) + "\""
	var fieldValue string
	print(fieldValueEncodedTrimmed)
	json.Unmarshal([]byte(fieldValueEncodedTrimmed), &fieldValue)

	return fieldValue
}

func removeHalfCutEscapeChar(str string) string {
	trailingBashslashCount := len(str) - len(strings.TrimRight(str, "\\"))
	if trailingBashslashCount%2 == 1 {
		str = str[0 : len(str)-1]
	}
	return str
}
