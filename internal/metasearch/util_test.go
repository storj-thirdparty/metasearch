// Copyright (C) 2025 Storj Labs, Inc.
// See LICENSE for copying information.

package metasearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseJSON(t *testing.T) {
	o, err := parseJSON(nil)
	require.NoError(t, err)
	require.Len(t, o, 0)

	s := ""
	o, err = parseJSON(&s)
	require.NoError(t, err)
	require.Len(t, o, 0)

	s = "foo"
	o, err = parseJSON(&s)
	require.Error(t, err)
	require.Len(t, o, 0)

	s = `{"foo": "bar"}`
	o, err = parseJSON(&s)
	require.NoError(t, err)
	require.Len(t, o, 1)
	require.Equal(t, "bar", o["foo"])
}

func TestNormalizePrefix(t *testing.T) {
	require.Equal(t, "", normalizeKeyPrefix(""))
	require.Equal(t, "", normalizeKeyPrefix("/"))
	require.Equal(t, "foo", normalizeKeyPrefix("/foo//"))
	require.Equal(t, "foo/bar", normalizeKeyPrefix("/foo/bar//"))
}

func TestShallowDeepMetadata(t *testing.T) {
	deepMeta := map[string]interface{}{
		"stringValue": "foo",
		"boolValue":   true,
		"intValue":    1,
		"arrayValue":  []interface{}{1, 2, 3},
		"objValue": map[string]interface{}{
			"foo": 1,
		},
	}

	shallowMeta, err := toShallowMetadata(deepMeta)
	require.NoError(t, err)

	deepMeta2, err := toDeepMetadata(shallowMeta)
	require.NoError(t, err)

	jsonMeta1, _ := json.Marshal(deepMeta)
	jsonMeta2, _ := json.Marshal(deepMeta2)
	require.JSONEq(t, string(jsonMeta1), string(jsonMeta2))
}

func TestSplitToJSONLeaves(t *testing.T) {
	input := `{
		"key1": "value1",
		"key2": {
			"key3": "value3",
			"key4": [
				1,
				2,
				{
					"key5": "value5",
					"key6": "value6"
				}
			]
		}
	}`

	expected := []string{
		`{"key1":"value1"}`,
		`{"key2":{"key3":"value3"}}`,
		`{"key2":{"key4":[1]}}`,
		`{"key2":{"key4":[2]}}`,
		`{"key2":{"key4":[{"key5":"value5"}]}}`,
		`{"key2":{"key4":[{"key6":"value6"}]}}`,
	}

	actual, err := splitToJSONLeaves(input)
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}
