package tritonhttp

import (
	"bufio"
	"errors"
	"strings"
	"unicode"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Header stores misc headers excluding "Host" and "Connection",
	// which are stored in special fields below.
	// Header keys are case-incensitive, and should be stored
	// in the canonical format in this map.
	Header map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

func isValidHeader(str string) bool {
	if len(str) == 0 {
		return false
	}

	if !isASCII(str) {
		return false
	}

	disallowedChars := []rune{
		'(', ')', '<', '>', '@', ',', ';', ':',
		'\\', '<', '>', '"', '/', '[', ']', '?', '=', '{', '}', ' ', '\t',
	}

	for _, r := range str {
		for _, d := range disallowedChars {
			if r == d {
				return false
			}
		}
	}

	return true
}

// ReadRequest tries to read the next valid request from br.
//
// If it succeeds, it returns the valid request read. In this case,
// bytesReceived should be true, and err should be nil.
//
// If an error occurs during the reading, it returns the error,
// and a nil request. In this case, bytesReceived indicates whether or not
// some bytes are received before the error occurs. This is useful to determine
// the timeout with partial request received condition.
func ReadRequest(br *bufio.Reader) (req *Request, bytesReceived bool, err error) {
	// Read start line
	req = &Request{
		Header: map[string]string{},
	}

	str, err := ReadLine(br)

	if err != nil {
		return nil, len(str) != 0, err
	}

	seg := strings.Split(str, " ")

	if len(seg) != 3 {
		return nil, true, errors.New("missing or extra http line")
	}

	req.Method = seg[0]
	req.URL = seg[1]
	req.Proto = seg[2]

	// Right now, only GET verb allowed
	// and this	 error seems to be expected to be handled during parsing
	if req.Method != "GET" {
		return nil, true, errors.New("only [GET] method allowed")
	}

	if !strings.HasPrefix(req.URL, "/") {
		return nil, true, errors.New("well-formed URL always starts with a /")
	}

	if req.Proto != "HTTP/1.1" {
		return nil, true, errors.New("only [HTTP/1.1] protocol allowed")
	}

	// Read headers
	for {
		str, err = ReadLine(br)

		if err != nil {
			return nil, true, err
		}

		if str == "" {
			break
		}

		idx := strings.Index(str, ":")

		if idx == -1 {
			return nil, true, errors.New("header should be separated by ':'")
		}

		key := str[:idx]
		value := str[idx+1:]

		// Test if key is valid
		// It should not contain some characters
		// value could be any
		if !isValidHeader(key) {
			return nil, true, errors.New("header key contain invalid character: " + key)
		}

		req.Header[CanonicalHeaderKey(key)] = strings.TrimSpace(value)
	}

	// Check required headers
	if val, ok := req.Header["Host"]; ok {
		req.Host = val
		delete(req.Header, "Host")
	} else {
		return nil, true, errors.New("host header not present")
	}

	// Handle special headers
	if val, ok := req.Header["Connection"]; ok {
		delete(req.Header, "Connection")
		if val == "close" {
			req.Close = true
		}
	}

	return req, true, nil
}
