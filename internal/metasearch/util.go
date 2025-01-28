package metasearch

import (
	"encoding/json"
)

func parseJSON(data string) (map[string]interface{}, error) {
	var meta map[string]interface{}
	err := json.Unmarshal([]byte(data), &meta)
	if err != nil {
		return nil, err
	}
	return meta, nil
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
