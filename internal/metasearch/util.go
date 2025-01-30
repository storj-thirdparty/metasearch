// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"encoding/json"
	"strings"
)

func parseJSON(data *string) (map[string]interface{}, error) {
	if data == nil || *data == "" {
		return nil, nil
	}
	var meta map[string]interface{}
	err := json.Unmarshal([]byte(*data), &meta)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func normalizeKeyPrefix(prefix string) string {
	for strings.HasPrefix(prefix, "/") {
		prefix = prefix[1:]
	}
	for strings.HasSuffix(prefix, "/") {
		prefix = prefix[:len(prefix)-1]
	}
	return prefix
}

// toShallowMetadata converts deep JSON structures into a shallow, string-to-string map.
// Example: {"foo":"1", "bar":[2]} is converted to {"foo":1, "json:bar":"[2]"}
func toShallowMetadata(meta map[string]interface{}) (map[string]string, error) {
	if len(meta) == 0 {
		return nil, nil
	}

	result := make(map[string]string)
	for k, v := range meta {
		if s, ok := v.(string); ok {
			result[k] = s
		} else {
			buf, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			result["json:"+k] = string(buf)
		}
	}
	return result, nil
}

// toDeepMetadata converts shallow, string-to-string metadata into a deep JSON object.
// Example: {"foo":1, "json:bar":"[2]"} is converted to {"foo":"1", "bar":[2]}
func toDeepMetadata(meta map[string]string) (map[string]interface{}, error) {
	if len(meta) == 0 {
		return nil, nil
	}

	result := make(map[string]interface{})
	for k, v := range meta {
		if strings.HasPrefix(k, "json:") {
			var j interface{}
			err := json.Unmarshal([]byte(v), &j)
			if err != nil {
				return nil, err
			}
			result[k[5:]] = j
		} else {
			result[k] = v
		}
	}
	return result, nil
}

func splitToJSONLeaves(j string) ([]string, error) {
	var obj interface{}
	if err := json.Unmarshal([]byte(j), &obj); err != nil {
		return nil, err
	}

	var leaves []string
	splitToLeafValues(obj, func(v interface{}) {
		if b, err := json.Marshal(v); err == nil {
			leaves = append(leaves, string(b))
		}
	})
	return leaves, nil
}

func splitToLeafValues(obj interface{}, add func(interface{})) []interface{} {
	switch obj := obj.(type) {
	case map[string]interface{}:
		for k, v := range obj {
			splitToLeafValues(v, func(v interface{}) {
				m := make(map[string]interface{})
				m[k] = v
				add(m)
			})
		}
	case []interface{}:
		for _, v := range obj {
			splitToLeafValues(v, func(v interface{}) {
				a := make([]interface{}, 1)
				a[0] = v
				add(a)
			})
		}
	default:
		add(obj)
	}
	return nil
}
