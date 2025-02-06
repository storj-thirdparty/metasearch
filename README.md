# Metasearch

## Overview

This repository implements a metadata search "plugin" for Storj. It has two main components:

- The [metasearch](cmd/metasearch) server contains the server code that should
be started alongside of an Storj Satellite installation

- The [metaclient](cmd/metaclient) CLI implements CRUD and search functionality
for the metadata.

## Design

The goal of the design was to provide a completely bolt-on solution with
minimal changes to the existing Storj architecture. The metasearch service can
be optionally installed after a succesful Storj installation, and without
having to patch the official Satellite images.

### Storing clear metadata

The metasearch service stores object metadata in the metainfo database in clear
text form. It is stored in a JSONB field, `clear_metadata` in the `objects` table,
and a CockroachDB GIN index is created for fast searching.

Metadata can be set in two ways:

1. By the metasearch server directly: the server receives the clear-text
   metadata from the client, and stores it directly into the database.
   It also encrypts the metadata and stores it in the `encrypted_metadata_*`
   fields for uplink compatiblity.

2. By `uplink cp` and similar commands: uplink only stores the metadata in encrypted form.
   To be able to decrypt it, the Metasearch server caches the access keys from
   clients, and migrates the encrypted metadata to `clear_metadata` for newly
   modified objects.

### Detecting metadata changes from uplink/satellite

Newly modified objects are detected using a second field,
`metasearch_queued_at` in the `objects` table. It is set to the current
timestamp by default whenever a record is modified in the DB. Metasearch polls
objects with non-NULL `metasearch_queued_at` fields in the DB and tries to
decode the encrypted metadata using its stored access keys. If an object cannot
be decrypted, metasearch will skip it, but it will try to reprocess it when it
receives a new access key from clients.

### Storing deep metadata structures

The metasearch service can store arbitrary JSON objects as metadata. Uplink, on
the other hand, only allows `map[string]string` objects. To make the most
backwards-compatible experience without modifying Storj/Satellite, the
following solution was implemented:

- The object metadata is of type `map[string]interface{}` on the metasearch
side. So the top-level objects is a dictonary, and its keys are always strings

- If the value of a top-level key is a string, it is stored the same way as
before: `{"foo":"bar"}` is stored as `{"foo":"bar"}` in both `clear_metadata`
and `encrypted_metadata`.

- If the value of a top-level key is not a string, it is stored as a JSON
serialized string, and metasearch prepends a `json:` prefix to the key. For
example, `{"foo":[1,2,3]"}` becomes `{"json:foo":"[1,2,3]"}` in
`encrypted_metadata`, and can be fetched/updated this way by uplink or S3
gateway.

## Server API

### Getting metadata

```
$ curl http://localhost:9998/metadata/bucketname/foo.txt -H "Authorization: Bearer $ACCESS_TOKEN"
{"foo":"bar","n":1}
```

### Setting metadata

```
$ curl -X PUT http://localhost:9998/metadata/bucketname/foo.txt \
  -H "Authorization: Bearer $ACCESS_TOKEN"
  -d '{"foo":"bar","n":2}'
```

### Deleting metadata
```
$ curl -X DELETE http://localhost:9998/metadata/bucketname/foo.txt \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

### Searching metadata

The query language consists of 3 parts:

1. Users can specify a `keyPrefix` and a `match` field in the search operation.
The `match` field is an arbitrary JSON value. The metasearch service fetches
a page of objects whose key matches the `keyPrefix` and whose JSON metadata
contains the `match` object. This step is analogous to DynamoDB's KeyCondition:
it finds objects quickly (using a GIN index and the `@>` operation on the JSONB
field), but it cannot contain complex expressions.

2. Users can also specify a `filter` field, which is an arbitrary
[JMESPath](https://jmespath.org) expression. `metasearch` applies this
expression to all items on the fetched page, and filters out values that return
a falsey value. This is analogous to DynamoDB's FilterCondition: it can do
powerful filtering, but it does not affect the cost of the search operation: it
is performed on the items that are already fetched. It is also possible to get
empty pages if the filter condition removes all items.

3. Users can also specify a `projection` field, which is also an arbitrary
JMESPath](https://jmespath.org) expression. `metasearch` transforms the
metadata objects with it, and returns the transformed JSON objects to the user.
This is useful if the user only wants to receive a small subset of the stored
metadata. It is analogous to DynamoDB's ProjectionExpression: it does not
affect the cost of the search operation, but may reduce the response payload.

Example:

```
$ curl http://localhost:9998/metasearch/bucketname \
  -H "Authorization: Bearer $ACCESS_TOKEN"
  -d '{"match":{"foo":"bar"}, "filter":"n>`1`", "projection":"foo"}' |
  | jq .

{
  "results": [
    {
      "path": "sj://bucketname/subdir/2.txt",
      "metadata": "bar"
    },
    {
      "path": "sj://bucketname/foo.txt",
      "metadata": "bar"
    }
  ]
}
```

## Metaclient CLI

The metaclient CLI is a small wrapper above the HTTP API. See `metaclient help` for details.

### Getting metadata

Example:

```
$ ./metaclient get sj://bucketname/subdir/1.txt
{
  "foo": "bar",
  "n": 1
}
```

### Setting metadata

Example:

```
$ ./metaclient set sj://bucketname/foo.txt -d '{"foo": "bar", "n": 2}'
```

### Deleting metadata

Example:

```
$ ./metaclient rm sj://bucketname/foo.txt
```

### Searching metadata

Example:

```
$ ./metaclient search sj://bucketname --match '{"foo":"bar"}' --filter 'n > `1`' --projection 'n'
[
  {
    "path": "sj://bucketname/subdir/2.txt",
    "metadata": 3
  },
  {
    "path": "sj://bucketname/foo.txt",
    "metadata": 2
  }
]
```

