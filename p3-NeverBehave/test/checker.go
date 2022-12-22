package test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"cse224/proj3/pkg/tritonhttp"
)

type ResponseChecker struct {
	StatusCode  int
	FilePath    string
	ContentType string
	Close       bool
}

type HeaderSpec struct {
	header string
	value  string // An empty value means we only check the header exists
}

var statusLineWant = map[int]string{
	200: "HTTP/1.1 200 OK",
	400: "HTTP/1.1 400 Bad Request",
	404: "HTTP/1.1 404 Not Found",
}

func (rc *ResponseChecker) Check(br *bufio.Reader) error {
	// Check status line
	line, err := tritonhttp.ReadLine(br)
	if err != nil {
		return err
	}
	if err := checkStatusLine(line, rc.StatusCode); err != nil {
		return err
	}

	// Check headers
	var specs []HeaderSpec
	connCloseHeader := []HeaderSpec{
		{"Connection", "close"},
	}
	switch rc.StatusCode {
	case 200:
		fi, err := os.Stat(rc.FilePath)
		if err != nil {
			return err
		}
		specs = []HeaderSpec{
			{"Content-Length", fmt.Sprint(fi.Size())},
			{"Content-Type", rc.ContentType},
			{"Date", ""},
			{"Last-Modified", ""},
		}
		if rc.Close {
			specs = append(connCloseHeader, specs...)
		}
	case 400:
		specs = []HeaderSpec{
			{"Connection", "close"},
			{"Date", ""},
		}
	case 404:
		specs = []HeaderSpec{
			{"Date", ""},
		}
		if rc.Close {
			specs = append(connCloseHeader, specs...)
		}
	default:
		return fmt.Errorf("unknown status code: %v", rc.StatusCode)
	}
	if err := checkHeaders(br, specs); err != nil {
		return err
	}

	// Check body
	if rc.StatusCode == 200 {
		if err := checkBody(br, rc.FilePath); err != nil {
			return err
		}
	}

	return nil
}

func checkStatusLine(line string, statusCode int) error {
	lineWant, ok := statusLineWant[statusCode]
	if !ok {
		return fmt.Errorf("unknown status code: %v", statusCode)
	}
	if line != lineWant {
		return fmt.Errorf("got: %q, want: %q", line, lineWant)
	}
	return nil
}

func checkHeaders(br *bufio.Reader, specs []HeaderSpec) error {
	for _, spec := range specs {
		line, err := tritonhttp.ReadLine(br)
		if err != nil {
			return err
		}
		if spec.value == "" {
			if !strings.HasPrefix(line, spec.header+": ") {
				return fmt.Errorf("got: %q, want: %q header", line, spec.header)
			}
		} else {
			lineWant := fmt.Sprintf("%v: %v", spec.header, spec.value)
			if line != lineWant {
				return fmt.Errorf("got: %q, want: %q", line, lineWant)
			}
		}
	}
	// Check header end
	line, err := tritonhttp.ReadLine(br)
	if err != nil {
		return err
	}
	if line != "" {
		return fmt.Errorf("got: %q, want: empty", line)
	}
	return nil
}

func checkBody(br *bufio.Reader, path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	lr := io.LimitReader(br, fi.Size())
	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, lr); err != nil {
		return err
	}
	bytesGot := buffer.Bytes()

	bytesWant, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	if !bytes.Equal(bytesGot, bytesWant) {
		return fmt.Errorf("body bytes are different from the file\ngot: %v bytes, want: %v bytes", len(bytesGot), len(bytesWant))
	}
	return nil
}
