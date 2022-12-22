package tritonhttp

import (
	"path/filepath"
	"testing"
)

const (
	contentTypeHTML = "text/html; charset=utf-8"
	contentTypeJPG  = "image/jpeg"
	contentTypePNG  = "image/png"
)

func normalizeTestdataPath(path string) (string, error) {
	basePath, err := filepath.Abs("testdata")
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	relPath, err := filepath.Rel(basePath, absPath)
	if err != nil {
		return "", err
	}
	return relPath, nil
}

func TestHandleGoodRequest(t *testing.T) {
	var tests = []struct {
		name             string
		req              *Request
		statusWant       int
		headersWant      []string
		headerValuesWant map[string]string
		filePathWant     string // relative to doc root
	}{
		{
			"OKBasic",
			&Request{
				Method: "GET",
				URL:    "/index.html",
				Proto:  "HTTP/1.1",
				Header: map[string]string{},
				Host:   "test",
				Close:  false,
			},
			200,
			[]string{
				"Date",
				"Last-Modified",
			},
			map[string]string{
				"Content-Type":   contentTypeHTML,
				"Content-Length": "12",
			},
			"index.html",
		},
		{
			"OKClose",
			&Request{
				Method: "GET",
				URL:    "/index.html",
				Proto:  "HTTP/1.1",
				Header: map[string]string{},
				Host:   "test",
				Close:  true,
			},
			200,
			[]string{
				"Date",
				"Last-Modified",
			},
			map[string]string{
				"Content-Type":   contentTypeHTML,
				"Content-Length": "12",
				"Connection":     "close",
			},
			"index.html",
		},
		{
			"OKDefaultRoot",
			&Request{
				Method: "GET",
				URL:    "/",
				Proto:  "HTTP/1.1",
				Header: map[string]string{},
				Host:   "test",
				Close:  false,
			},
			200,
			[]string{
				"Date",
				"Last-Modified",
			},
			map[string]string{
				"Content-Type":   contentTypeHTML,
				"Content-Length": "12",
			},
			"index.html",
		},
		{
			"NotFoundBasic",
			&Request{
				Method: "GET",
				URL:    "/notexist.html",
				Proto:  "HTTP/1.1",
				Header: map[string]string{},
				Host:   "test",
				Close:  false,
			},
			404,
			[]string{
				"Date",
			},
			map[string]string{},
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				Addr:    ":0",
				DocRoot: "testdata",
			}
			res := s.HandleGoodRequest(tt.req)
			if res.StatusCode != tt.statusWant {
				t.Fatalf("status code got: %v, want: %v", res.StatusCode, tt.statusWant)
			}
			for _, h := range tt.headersWant {
				if _, ok := res.Header[h]; !ok {
					t.Fatalf("missing header %q", h)
				}
			}
			for h, vWant := range tt.headerValuesWant {
				v, ok := res.Header[h]
				if !ok {
					t.Fatalf("missing header %q", h)
				}
				if v != vWant {
					t.Fatalf("header %q value got: %q, want %q", h, v, vWant)
				}
			}
			if tt.filePathWant != "" {
				// Case with file to serve
				filePath, err := normalizeTestdataPath(res.FilePath)
				if err != nil {
					t.Fatalf("invalid file path: %q", res.FilePath)
				}
				if filePath != tt.filePathWant {
					t.Fatalf("file path (relative to testdata/) got: %q, want: %q", filePath, tt.filePathWant)
				}
			} else {
				// Case with no file to serve
				if res.FilePath != tt.filePathWant {
					t.Fatalf("file path got: %q, want: %q", res.FilePath, tt.filePathWant)
				}
			}
		})
	}
}
