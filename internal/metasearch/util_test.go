package metasearch

import (
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
