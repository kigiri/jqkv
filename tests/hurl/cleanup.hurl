DELETE http://127.0.0.1:3000/user/2
HTTP 204

GET http://127.0.0.1:3000/user/2
HTTP 404

DELETE http://127.0.0.1:3000/org/acme/user/1/message/1
HTTP 204

GET http://127.0.0.1:3000/org/acme/user/1/message/1
HTTP 404
