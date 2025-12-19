# Insert test data
POST http://127.0.0.1:3000/user/1
Content-Type: application/json
{
  "name": "alice",
  "role": "admin",
  "score": 10
}
HTTP 204

POST http://127.0.0.1:3000/user/2
Content-Type: application/json
{
  "name": "bob",
  "role": "member",
  "score": 7
}
HTTP 204

POST http://127.0.0.1:3000/group/alpha
Content-Type: application/json
{
  "name": "core-team",
  "active": true
}
HTTP 204

POST http://127.0.0.1:3000/org/acme/user/3
Content-Type: application/json
{
  "name": "cara",
  "role": "member",
  "score": 4
}
HTTP 204

POST http://127.0.0.1:3000/org/acme/user/4
Content-Type: application/json
{
  "name": "dana",
  "role": "admin",
  "score": 9
}
HTTP 204

POST http://127.0.0.1:3000/org/acme/group/ops
Content-Type: application/json
{
  "name": "ops",
  "active": true
}
HTTP 204

POST http://127.0.0.1:3000/org/acme/user/1/message/1
Content-Type: application/json
{
  "text": "hello"
}
HTTP 204

POST http://127.0.0.1:3000/org/acme/user/1/message/2
Content-Type: application/json
{
  "text": "bye"
}
HTTP 204

# Fetch a single key
GET http://127.0.0.1:3000/user/1
HTTP 200
[Asserts]
jsonpath "$.name" == "alice"
jsonpath "$.role" == "admin"

# Query by prefix
GET http://127.0.0.1:3000/user/?q=._key
HTTP 200
[Asserts]
jsonpath "$[0]" == "user/1"
jsonpath "$[1]" == "user/2"

# Nested group prefix query
GET http://127.0.0.1:3000/org/acme/group/?q=._key
HTTP 200
[Asserts]
jsonpath "$[0]" == "org/acme/group/ops"

# Project multiple fields
GET http://127.0.0.1:3000/user/?q={k:._key,r:.role}
HTTP 200
[Asserts]
jsonpath "$[0].k" == "user/1"
jsonpath "$[0].r" == "admin"

# Nested prefix query
GET http://127.0.0.1:3000/org/acme/user/1/message/?q=.text
HTTP 200
[Asserts]
jsonpath "$[0]" == "hello"
jsonpath "$[1]" == "bye"

# Aggregation using inputs
POST http://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "reduce inputs as $row (0; . + $row.score)",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == 13

# Aggregation with filter
POST http://127.0.0.1:3000/
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
POST http://127.0.0.1:3000/
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
POST http://127.0.0.1:3000/
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
POST http://127.0.0.1:3000/
Content-Type: application/json
{
  "prefixes": ["org/acme/user/"],
  "query": "[inputs | select(.role == \"member\")] | length",
  "from": 0
}
HTTP 200
[Asserts]
jsonpath "$[0]" == 1

# Collect roles from users only
POST http://127.0.0.1:3000/
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
POST http://127.0.0.1:3000/
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
GET http://127.0.0.1:3000/user/404
HTTP 404
[Asserts]
jsonpath "$.code" == "not_found"

# Invalid query should return JSON error
GET http://127.0.0.1:3000/user/?q=bad(
HTTP 400
[Asserts]
jsonpath "$.code" == "invalid_query"
