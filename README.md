# ECE419 Project

Contains code used in the ECE419: Distributed Systems course.


## Key and Value System

### Keys

- max 20 bytes (thus 20 chars)
- no whitespace
- no newlines

### Values

- max 120 kbytes (122880 bytes) (thus 122880 chars)
- no newlines


## KVServer Network Communication API

Note: All JSON bodies must be on a single line! No newlines allowed!
All messages contain a single empty line at the end to indicate the end of the
message.

### Request: Get a value for a key

Contains:

1: request (string): "GET"
2: key (string): the key to look up

Examples:

```
GET
myKey
```

### Request: Put a value for a key

Contains:

1: request (string): "PUT"
2: key (string): the key to update the value for
3: data (string): the value for the key
    (no data or empty string means delete the key)

Examples:

```
PUT
myKey
new data
```

```
PUT
myKey
```

### Response: For a put or get request

Contains:

1: result (string): one of "GET_SUCCESS", "GET_ERROR", "PUT_SUCCESS",
    "PUT_UPDATE", "PUT_ERROR", "DELETE_SUCCESS", "DELETE_ERROR"
2: key (string): the key pertaining to the request (if successful)
3: data (string): the value pertaining to the request (if successful)
2: error (string): reason for failure (if unsuccessful)

Examples:

```
GET_SUCCESS
myKey
some data
```
Note: All success responses are the same format.

```
GET_ERROR
Key does not exist
```
