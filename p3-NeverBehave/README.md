# TritonHTTP

## Spec Summary

Here we provide a concise summary of the TritonHTTP spec. You should read the spec doc for more details and clarifications.

### HTTP Messages

TritonHTTP follows the [general HTTP message format](https://developer.mozilla.org/en-US/docs/Web/HTTP/Messages). And it has some further specifications:

- HTTP version supported: `HTTP/1.1`
- Request method supported: `GET`
- Response status supported:
  - `200 OK`
  - `400 Bad Request`
  - `404 Not Found`
- Request headers:
  - `Host` (required)
  - `Connection` (optional, `Connection: close` has special meaning influencing server logic)
  - Other headers are allowed, but won't have any effect on the server logic
- Response headers:
  - `Date` (required)
  - `Last-Modified` (required for a `200` response)
  - `Content-Type` (required for a `200` response)
  - `Content-Length` (required for a `200` response)
  - `Connection: close` (required in response for a `Connection: close` request, or for a `400` response)
  - Response headers should be written in sorted order for the ease of testing

### Server Logic

When to send a `200` response?
- When a valid request is received, and the requested file can be found.

When to send a `404` response?
- When a valid request is received, and the requested file cannot be found or is not under the doc root.

When to send a `400` response?
- When an invalid request is received.
- When timeout occurs and a partial request is received.

When to close the connection?
- When timeout occurs and no partial request is received.
- When EOF occurs.
- After sending a `400` response.
- After handling a valid request with a `Connection: close` header.

When to update the timeout?
- When trying to read a new request.

What is the timeout value?
- 5 seconds.

## Implementation

Please limit your implimentation to the following files, because we'll only copy over these files for grading:
- `pkg/tritonhttp/`
  - `request.go`
  - `response.go`
  - `server.go`

There are some utility functions defined in `pkg/tritonhttp/util.go` that you might find useful.

You can (and are encouraged to) extend the tests (both unit and e2e tests) for your local testing. We'll use the same testing framework for grading, just with different test cases.

In terms of effort level, note that our solution involved writing 293 lines of new code (127 in server.go, 102 in request.go, and 64 in response.go).

## Usage

Install the `httpd` command to a local `bin` directory:
```
make install
ls bin
```

Check the command help message:
```
bin/httpd -h
```

An alternative way to run the command:
```
go run cmd/httpd/main.go -h
```

## Testing

### Sanity Checking

We provide 2 simple examples for your sanity checking.

First you could run an example with the default server:
```
make run-default
```

This example uses the Golang standard library HTTP server to serve the website, and it doesn't rely on your implementation of TritonHTTP at all. So you shall be able to run it with the starter code right away. Open the link from output in a browser, and you shall see a test website.

Once you have a working implementation of TritonHTTP, you could run another example:
```
make run-tritonhttp
```

Again, you could use a browser to check the test website served.

### Unit Testing

Unit tests don't involve any networking. They check the logic of the main parts of your implementation.

To run all the unit tests:
```
make unit-test
```

### End-to-End Testing

End-to-end tests involve runing a server locally and testing by communicating with this server.

To run all the end-to-end tests:
```
make e2e-test
```

### Manual Testing

For manutal testing, we recommend using `nc`.

In one terminal, start the TritonHTTP server:
```
go run cmd/httpd/main.go -port 8080 -doc_root test/testdata/htdocs
```

In another terminal, use `nc` to send request to it:
```
cat test/testdata/requests/single/OKBasic.txt | nc localhost 8080
```

You'll see the response printed out. And you could look at your server's logging to debug.

## Submission

Either submit through GitHub, or:
```
make submission
```

And upload the generated `submission.zip` file to Gradescope.
