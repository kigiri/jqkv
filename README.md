# Simple K/V json store with JQ as query language

the plan:
- JQ query syntax for a LMDB kv store over an API.

why rust:
- low memory footprint
- good performances
- has good bindings for LMDB and jaq as JQ rust native alternative
- Good featurefull http servers (hyper)

## TODO
### mini
- [x] parse a JQ query over a bunch of lines of json
- [x] load a LMDB database
- [x] function to insert data (with metadata) in LMDB
- [x] populate a test LMDB file with JSON and run JQ over it (output in std)
- [x] put all the logic behind a single route with hyper
- [ ] some kind of security 
### pro
- [x] Use the metadata table for time based filtering
- [x] add tokio thread for blocking long queries
- [ ] stream the response directly from jaq to the response
- [x] basic pre-filtering based on key prefix (should be easy, lmdb feature)
- [ ] regex pre-filtering on keys
- [ ] some kind of polling or something to trigger updates over SSR
- [ ] handle pagination with a CURSOR (not sure the proper way to do this, hold for the query for a bit ?)
- [x] `messagepack` or `cbor` support (messagepack smaller, simpler, cbor native support in jaq)
- [x] ~~lmdb compression (lz4-hc)~~ 30% slower for only 20% smaller
- [ ] Explore compression on the whole db level, if possible would probably do a much better difference.
- [ ] add a cache to avoid re-parsing JQ on every queries for common queries (not a big cost)
- [ ] Set etag using the metadata db
- [x] add route to get a single element from it's id
- [ ] Limit upload size to something reasonable (?)
- [ ] improve time range performances
  - [ ] move the "meta" timestamp to a data prefix
  - [ ] create a index by timestamp to be able to range on time
### max
- [ ] default web UI to explore the data
- [ ] generate keys prefixes
- [ ] generate matching schemas
- [ ] llm / language server style auto-complete on the webUI query builder
- [ ] update jaq to new version once it's stabilize and use first class cbor support (if this allow lazy access to values and avoid reading the whole buffer if we don't need to, this could be the biggest improvement)
- [ ] make sure we only do the work we need for big queries (big speed ups)
