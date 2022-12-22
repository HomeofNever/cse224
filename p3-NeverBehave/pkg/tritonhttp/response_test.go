package tritonhttp

import (
	"bytes"
	"os"
	"testing"
)

func TestWriteStatusLine(t *testing.T) {
	var tests = []struct {
		name string
		res  *Response
		want string
	}{
		{
			"OK",
			&Response{
				StatusCode: 200,
				Proto:      "HTTP/1.1",
			},
			"HTTP/1.1 200 OK\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			if err := tt.res.WriteStatusLine(&buffer); err != nil {
				t.Fatal(err)
			}
			got := buffer.String()
			if got != tt.want {
				t.Fatalf("got: %q, want: %q", got, tt.want)
			}
		})
	}
}

func TestWriteSortedHeaders(t *testing.T) {
	var tests = []struct {
		name string
		res  *Response
		want string
	}{
		{
			"Basic",
			&Response{
				Header: map[string]string{
					"Connection": "close",
					"Date":       "foobar",
					"Misc":       "hello world",
				},
			},
			"Connection: close\r\n" +
				"Date: foobar\r\n" +
				"Misc: hello world\r\n" +
				"\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			if err := tt.res.WriteSortedHeaders(&buffer); err != nil {
				t.Fatal(err)
			}
			got := buffer.String()
			if got != tt.want {
				t.Fatalf("got: %q, want: %q", got, tt.want)
			}
		})
	}
}

func TestWriteBody(t *testing.T) {
	var tests = []struct {
		name string
		path string
	}{
		{
			"Basic",
			"testdata/index.html",
		},
		{
			"NoBody",
			"", // An empty path means there is no body to write
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := &Response{
				FilePath: tt.path,
			}
			var buffer bytes.Buffer
			if err := res.WriteBody(&buffer); err != nil {
				t.Fatal(err)
			}
			bytesGot := buffer.Bytes()

			// No path, no bytes
			var bytesWant []byte
			if tt.path != "" {
				var err error
				if bytesWant, err = os.ReadFile(tt.path); err != nil {
					t.Fatal(err)
				}
			}

			if !bytes.Equal(bytesGot, bytesWant) {
				if len(bytesWant) <= 128 {
					// For small file, show the bytes
					t.Fatalf("\ngot: %q\nwant: %q", bytesGot, bytesWant)
				} else {
					// Otherwise, just show number of bytes
					t.Fatalf(
						"bytes written are different from the file\ngot: %v bytes, want: %v bytes",
						len(bytesGot),
						len(bytesWant),
					)
				}
			}
		})
	}
}
