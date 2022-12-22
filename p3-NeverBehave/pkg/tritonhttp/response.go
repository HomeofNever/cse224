package tritonhttp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

type Response struct {
	StatusCode int    // e.g. 200
	Proto      string // e.g. "HTTP/1.1"

	// Header stores all headers to write to the response.
	// Header keys are case-incensitive, and should be stored
	// in the canonical format in this map.
	Header map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

// Write writes the res to the w.
func (res *Response) Write(w io.Writer) error {
	if err := res.WriteStatusLine(w); err != nil {
		return err
	}
	if err := res.WriteSortedHeaders(w); err != nil {
		return err
	}
	if err := res.WriteBody(w); err != nil {
		return err
	}
	return nil
}

func codeMatch(status int) (string, error) {
	switch status {
	case 200:
		return "OK", nil
	case 404:
		return "Not Found", nil
	case 400:
		return "Bad Request", nil
	default:
		return "", errors.New("not implemented")
	}
}

// WriteStatusLine writes the status line of res to w, including the ending "\r\n".
// For example, it could write "HTTP/1.1 200 OK\r\n".
func (res *Response) WriteStatusLine(w io.Writer) error {
	status, err := codeMatch(res.StatusCode)
	if err != nil {
		return err
	}

	str := fmt.Sprintf("%v %v %v\r\n", res.Proto, res.StatusCode, status)
	w.Write([]byte(str))

	return nil
}

// WriteSortedHeaders writes the headers of res to w, including the ending "\r\n".
// For example, it could write "Connection: close\r\nDate: foobar\r\n\r\n".
// For HTTP, there is no need to write headers in any particular order.
// TritonHTTP requires to write in sorted order for the ease of testing.
func (res *Response) WriteSortedHeaders(w io.Writer) error {
	keys := make([]string, 0, len(res.Header))
	for k := range res.Header {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		v := fmt.Sprintf("%v: %v\r\n", k, res.Header[k])
		w.Write([]byte(v))
	}

	w.Write([]byte("\r\n"))

	return nil
}

// WriteBody writes res' file content as the response body to w.
// It doesn't write anything if there is no file to serve.
func (res *Response) WriteBody(w io.Writer) error {
	if res.FilePath != "" {
		file, err := os.ReadFile(res.FilePath)
		if err != nil {
			return err
		}

		w.Write(file)
	}

	return nil
}
