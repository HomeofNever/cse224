package test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const (
	testPort        = 8080
	testDocRoot     = "testdata/htdocs"
	serverSetupTime = 1 // second

	contentTypeHTML = "text/html; charset=utf-8"
	contentTypeJPG  = "image/jpeg"
	contentTypePNG  = "image/png"
)

// Global test setup.
// See https://pkg.go.dev/testing#hdr-Main
func TestMain(m *testing.M) {
	// Start the test server before running any test cases
	serverCmd := exec.Command(
		"_bin/httpd",
		"-port", strconv.Itoa(testPort),
		"-doc_root", testDocRoot,
	)
	if err := serverCmd.Start(); err != nil {
		log.Fatal(err)
	}

	// Wait a little bit for the server to set up.
	// Otherwise we might get "connection refused".
	time.Sleep(serverSetupTime * time.Second)

	// Start running the test cases
	code := m.Run()

	// Kill the test server process
	serverCmd.Process.Kill()

	os.Exit(code)
}

func TestSingleRequest(t *testing.T) {
	var tests = []struct {
		name       string
		resChecker *ResponseChecker
	}{
		{
			"OKBasic",
			&ResponseChecker{
				StatusCode:  200,
				FilePath:    filepath.Join(testDocRoot, "index.html"),
				ContentType: contentTypeHTML,
				Close:       true,
			},
		},
		{
			"OKTimeout",
			&ResponseChecker{
				StatusCode:  200,
				FilePath:    filepath.Join(testDocRoot, "index.html"),
				ContentType: contentTypeHTML,
				Close:       false,
			},
		},
		{
			"OKHeader",
			&ResponseChecker{
				StatusCode:  200,
				FilePath:    filepath.Join(testDocRoot, "UCSD_Seal.png"),
				ContentType: contentTypePNG,
				Close:       true,
			},
		},
		{
			"OKJPG",
			&ResponseChecker{
				StatusCode:  200,
				FilePath:    filepath.Join(testDocRoot, "kitten.jpg"),
				ContentType: contentTypeJPG,
				Close:       false,
			},
		},
		{
			"BadRequestHost",
			&ResponseChecker{
				StatusCode: 400,
			},
		},
		{
			"BadRequestMethod",
			&ResponseChecker{
				StatusCode: 400,
			},
		},
		{
			"BadRequestMissingHead",
			&ResponseChecker{
				StatusCode: 400,
			},
		},
		{
			"BadRequestProtocol",
			&ResponseChecker{
				StatusCode: 400,
			},
		},
		{
			"BadRequestURL",
			&ResponseChecker{
				StatusCode: 400,
			},
		},
		{
			"BadRequestBasic",
			&ResponseChecker{
				StatusCode: 400,
			},
		},
		{
			"BadRequestTimeout",
			&ResponseChecker{
				StatusCode: 400,
			},
		},
		{
			"NotFoundBasic",
			&ResponseChecker{
				StatusCode: 404,
				Close:      true,
			},
		},
		{
			"NotFoundTimeout",
			&ResponseChecker{
				StatusCode: 404,
				Close:      false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqPath := filepath.Join("testdata/requests/single", tt.name+".txt")
			resPath := filepath.Join("testdata/responses/single", tt.name+".dat")

			c := &Client{Port: testPort}
			defer c.Close()
			if err := c.Dial(); err != nil {
				t.Fatal(err)
			}
			if err := c.SendRequestFromFile(reqPath); err != nil {
				t.Fatal(err)
			}
			if err := c.ReceiveResponseToFile(resPath); err != nil {
				t.Fatal(err)
			}

			f, err := os.Open(resPath)
			if err != nil {
				t.Fatal(err)
			}
			br := bufio.NewReader(f)
			if err := tt.resChecker.Check(br); err != nil {
				t.Fatal(err)
			}
			if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatalf("response has extra bytes when it should end")
			}
		})
	}
}

func TestPipelineRequest(t *testing.T) {
	var tests = []struct {
		name        string
		resCheckers []*ResponseChecker
	}{
		{
			"OKOKOK",
			[]*ResponseChecker{
				{
					StatusCode:  200,
					FilePath:    filepath.Join(testDocRoot, "index.html"),
					ContentType: contentTypeHTML,
					Close:       false,
				},
				{
					StatusCode:  200,
					FilePath:    filepath.Join(testDocRoot, "index.html"),
					ContentType: contentTypeHTML,
					Close:       false,
				},
				{
					StatusCode:  200,
					FilePath:    filepath.Join(testDocRoot, "index.html"),
					ContentType: contentTypeHTML,
					Close:       true,
				},
			},
		},
		{
			"OKBadRequestOK",
			[]*ResponseChecker{
				{
					StatusCode:  200,
					FilePath:    filepath.Join(testDocRoot, "index.html"),
					ContentType: contentTypeHTML,
					Close:       false,
				},
				{
					StatusCode: 400,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqPath := filepath.Join("testdata/requests/pipeline", tt.name+".txt")
			resPath := filepath.Join("testdata/responses/pipeline", tt.name+".dat")

			c := &Client{Port: testPort}
			defer c.Close()
			if err := c.Dial(); err != nil {
				t.Fatal(err)
			}
			if err := c.SendRequestFromFile(reqPath); err != nil {
				t.Fatal(err)
			}
			if err := c.ReceiveResponseToFile(resPath); err != nil {
				t.Fatal(err)
			}

			f, err := os.Open(resPath)
			if err != nil {
				t.Fatal(err)
			}
			br := bufio.NewReader(f)
			for _, resChecker := range tt.resCheckers {
				if err := resChecker.Check(br); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatalf("response has extra bytes when it should end")
			}
		})
	}
}

type concurrentTestSpec struct {
	// reqPath is the path to the request file to send.
	reqPath string

	resChecker *ResponseChecker
}

// To test concurrent request handling, we send 2 requests.
// The first one doesn't have the "Connection: close" header,
// so it would hang until server timeout (~5s).
// At the same time, we send a second request to the server.
// We check the responses from both reqeusts as a verification.
func TestConcurrentRequest(t *testing.T) {
	var tests = []struct {
		name  string
		specs []*concurrentTestSpec
	}{
		{
			"OKOK",
			[]*concurrentTestSpec{
				{
					"testdata/requests/single/OKTimeout.txt",
					&ResponseChecker{
						StatusCode:  200,
						FilePath:    filepath.Join(testDocRoot, "index.html"),
						ContentType: contentTypeHTML,
						Close:       false,
					},
				},
				{
					"testdata/requests/single/OKBasic.txt",
					&ResponseChecker{
						StatusCode:  200,
						FilePath:    filepath.Join(testDocRoot, "index.html"),
						ContentType: contentTypeHTML,
						Close:       true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errChan := make(chan error)
			for i, spec := range tt.specs {
				reqPath := spec.reqPath
				resPath := filepath.Join("testdata/responses/concurrent", fmt.Sprintf("%v%v.dat", tt.name, i))

				// Send requests and check responses concurrently
				go func(spec *concurrentTestSpec) {
					c := &Client{Port: testPort}
					defer c.Close()
					if err := c.Dial(); err != nil {
						errChan <- err
						return
					}
					if err := c.SendRequestFromFile(reqPath); err != nil {
						errChan <- err
						return
					}
					if err := c.ReceiveResponseToFile(resPath); err != nil {
						errChan <- err
						return
					}

					f, err := os.Open(resPath)
					if err != nil {
						errChan <- err
						return
					}
					br := bufio.NewReader(f)
					if err := spec.resChecker.Check(br); err != nil {
						errChan <- err
						return
					}
					if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
						errChan <- fmt.Errorf("response has extra bytes when it should end")
						return
					}
					errChan <- nil
				}(spec)
			}

			for range tt.specs {
				err := <-errChan
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
