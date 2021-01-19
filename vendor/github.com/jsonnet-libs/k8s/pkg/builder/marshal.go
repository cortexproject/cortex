package builder

import (
	"encoding/json"
	"fmt"
)

func Marshal(name string, ptr interface{}) Type {
	// get rid of custom types, but retain metadata (json)
	jsonData, err := json.Marshal(ptr)
	if err != nil {
		panic(err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(jsonData, &data); err != nil {
		panic(err)
	}

	return marshal(name, data)
}

func marshal(name string, ptr interface{}) Type {
	switch t := ptr.(type) {
	case int:
		return Int(name, t)
	case float64:
		return Int(name, int(t))
	case string:
		return String(name, t)
	case bool:
		return Bool(name, t)
	case map[string]interface{}:
		childs := []Type{}
		for k, v := range t {
			childs = append(childs, marshal(k, v))
		}
		return Object(name, childs...)
	case []interface{}:
		childs := []Type{}
		for _, v := range t {
			childs = append(childs, marshal("", v))
		}
		return List(name, childs...)
	default:
		panic(fmt.Sprintf("unsupported type: %T", ptr))
	}
}
