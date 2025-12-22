# Insert test data
POST https://127.0.0.1:3000/user/1
Content-Type: application/json
{
  "name": "alice",
  "role": "admin",
  "score": 10
}
HTTP 204

POST https://127.0.0.1:3000/user/2
Content-Type: application/json
{
  "name": "bob",
  "role": "member",
  "score": 7
}
HTTP 204

POST https://127.0.0.1:3000/group/alpha
Content-Type: application/json
{
  "name": "core-team",
  "active": true
}
HTTP 204

POST https://127.0.0.1:3000/org/acme/user/3
Content-Type: application/json
{
  "name": "cara",
  "role": "member",
  "score": 4
}
HTTP 204

POST https://127.0.0.1:3000/org/acme/user/4
Content-Type: application/json
{
  "name": "dana",
  "role": "admin",
  "score": 9
}
HTTP 204

POST https://127.0.0.1:3000/org/acme/user/5
Content-Type: application/json
{
  "name": "erin",
  "role": "member",
  "score": 5
}
HTTP 204

POST https://127.0.0.1:3000/org/acme/group/ops
Content-Type: application/json
{
  "name": "ops",
  "active": true
}
HTTP 204

POST https://127.0.0.1:3000/org/acme/user/1/message/1
Content-Type: application/json
{
  "text": "hello"
}
HTTP 204

POST https://127.0.0.1:3000/org/acme/user/1/message/2
Content-Type: application/json
{
  "text": "bye"
}
HTTP 204

# Fetch a single key
GET https://127.0.0.1:3000/user/1
HTTP 200
[Asserts]
jsonpath "$.name" == "alice"
jsonpath "$.role" == "admin"

# Query by prefix
GET https://127.0.0.1:3000/user/?q=._key
HTTP 200
[Asserts]
jsonpath "$[0]" == "user/1"
jsonpath "$[1]" == "user/2"

# Nested group prefix query
GET https://127.0.0.1:3000/org/acme/group/?q=._key
HTTP 200
[Asserts]
jsonpath "$[0]" == "org/acme/group/ops"

# Unchanged update should return 304
POST https://127.0.0.1:3000/org/acme/user/5
Content-Type: application/json
{
  "name": "erin",
  "role": "member",
  "score": 5
}
HTTP 304

# Capture timestamp before update
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "inputs | select(._key == \"org/acme/user/5\") | ._at",
  "from": 0
}
HTTP 200
[Captures]
user5_before: jsonpath "$[0]"

# Updated value should return 204
POST https://127.0.0.1:3000/org/acme/user/5
Content-Type: application/json
{
  "name": "erin",
  "role": "member",
  "score": 6
}
HTTP 204

# Timestamp should change after update
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "inputs | select(._key == \"org/acme/user/5\") | ._at",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" != {{user5_before}}

# Project multiple fields
GET https://127.0.0.1:3000/user/?q={k:._key,r:.role}
HTTP 200
[Asserts]
jsonpath "$[0].k" == "user/1"
jsonpath "$[0].r" == "admin"

# Nested prefix query
GET https://127.0.0.1:3000/org/acme/user/1/message/?q=.text
HTTP 200
[Asserts]
jsonpath "$[0]" == "hello"
jsonpath "$[1]" == "bye"

# Aggregation using inputs
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "reduce inputs as $row (0; . + $row.score)",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == 19

# Aggregation with filter
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "reduce inputs as $row (0; . + (if $row.role == \"admin\" then 1 else 0 end))",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == 1

# Filtered projection using inputs
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "inputs | select(.role == \"admin\") | .name",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == "dana"

# Highest score user
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "[inputs] | sort_by(.score) | last",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0].name" == "dana"
jsonpath "$[0].score" == 9

# Count users matching a filter
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "[inputs | select(.role == \"member\")] | length",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == 2

# Collect roles from users only
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "inputs | select(.role != null) | .role",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == "member"
jsonpath "$[1]" == "admin"

# Multi-prefix search via POST /
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["user/", "group/", "org/acme/"],
  "query": "._key",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == "user/1"
jsonpath "$[1]" == "user/2"
jsonpath "$[2]" == "group/alpha"

# Missing key should return JSON error
GET https://127.0.0.1:3000/user/404
HTTP 404
[Asserts]
jsonpath "$.code" == "not_found"

# Invalid JSON body should return JSON error
POST https://127.0.0.1:3000/
Content-Type: application/json
`{"prefixes":[`
HTTP 400
[Asserts]
jsonpath "$.code" == "bad_request"

# Missing fields should return JSON error
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["user/"]
}
HTTP 400
[Asserts]
jsonpath "$.code" == "bad_request"

# Invalid query via POST should return JSON error
POST https://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["user/"],
  "query": "bad(",
  "from": 0
}
HTTP 400
[Asserts]
jsonpath "$.code" == "invalid_query"

# Invalid JSON on key write should return JSON error
POST https://127.0.0.1:3000/user/1
Content-Type: application/json
`{"name":`
HTTP 400
[Asserts]
jsonpath "$.code" == "bad_request"

# Missing key for delete should return JSON error
DELETE https://127.0.0.1:3000/
HTTP 400
[Asserts]
jsonpath "$.code" == "missing_key"

# Method not allowed
PUT https://127.0.0.1:3000/user/1
HTTP 405
[Asserts]
jsonpath "$.code" == "method_not_allowed"

# Payload too large
POST https://127.0.0.1:3000/user/1
Content-Type: application/octet-stream
file, /tmp/jqkv-tests/large.bin;
HTTP 413
[Asserts]
jsonpath "$.code" == "payload_too_large"

# Invalid query should return JSON error
GET https://127.0.0.1:3000/user/?q=bad(
HTTP 400
[Asserts]
jsonpath "$.code" == "invalid_query"
