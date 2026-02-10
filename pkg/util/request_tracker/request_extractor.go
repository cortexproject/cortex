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

type ApiExtractor struct{}

type InstantQueryExtractor struct{}

type RangedQueryExtractor struct{}

func generateCommonMap(r *http.Request) map[string]interface{} {
	ctx := r.Context()
	entryMap := make(map[string]interface{})
	entryMap["timestampSec"] = time.Now().Unix()
	entryMap["Path"] = r.URL.Path
	entryMap["Method"] = r.Method
	entryMap["TenantID"], _ = users.TenantID(ctx)
	entryMap["RequestID"] = requestmeta.RequestIdFromContext(ctx)
	entryMap["UserAgent"] = r.Header.Get("User-Agent")
	entryMap["DashboardUID"] = r.Header.Get("X-Dashboard-UID")
	entryMap["PanelId"] = r.Header.Get("X-Panel-Id")

	return entryMap
}

func (e *ApiExtractor) Extract(r *http.Request) []byte {
	entryMap := generateCommonMap(r)
	entryMap["limit"] = r.URL.Query().Get("limit")
	entryMap["start"] = r.URL.Query().Get("start")
	entryMap["end"] = r.URL.Query().Get("end")

	matches := r.URL.Query()["match[]"]
	entryMap["numberOfMatches"] = len(matches)
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

func trimForJsonMarshal(field string, size int) string {
	return trimForJsonMarshalRecursive(field, size, 0, size)
}

func trimForJsonMarshalRecursive(field string, size int, repeatCount int, repeatSize int) string {
	//Should only repeat once since were over slightly over cutting based on the encoded size if we miss once
	if repeatCount > 1 {
		return ""
	}

	fieldTrimmed := trimStringByBytes(field, repeatSize)
	fieldEncoded, err := json.Marshal(fieldTrimmed)
	if err != nil {
		return ""
	}
	if len(fieldEncoded) > size {
		repeatSize = repeatSize - (len(fieldEncoded) - repeatSize)
		return trimForJsonMarshalRecursive(fieldTrimmed, size, repeatCount+1, repeatSize)
	}
	return fieldTrimmed
}
