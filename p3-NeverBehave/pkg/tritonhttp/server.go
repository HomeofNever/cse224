package tritonhttp

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// DocRoot specifies the path to the directory to serve static files from.
	DocRoot string
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("Unable to listen Addr: %v", s.Addr)
	}

	log.Printf("Server started at %v", s.Addr)

	defer l.Close()
	defer fmt.Println("Server Stopped Listening.")

	for {
		conn, err := l.Accept()

		if err == nil {
			go s.HandleConnection(conn)
		}
	}
}

// HandleConnection reads requests from the accepted conn and handles them.
func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Set timeout
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Try to read next request
		req, byteReceived, err := ReadRequest(reader)

		// Handle EOF
		if err == io.EOF {
			// Connection Closed
			return
		}

		resp := &Response{
			Proto: "HTTP/1.1",
			Header: map[string]string{
				"Date": FormatTime(time.Now()),
			},
		}
		resp.Request = req

		switch {
		case byteReceived && os.IsTimeout(err):
			{
				// Handle timeout
				resp.HandleBadRequest()
				resp.Write(conn)
				fmt.Printf("(400) Partial Request received and timeout\n")
				return
			}
		case os.IsTimeout(err):
			{
				return // Close Connection
			}
		case err != nil:
			{
				// Invalid Format?
				// Return 400
				fmt.Printf("(400) Invalid Format: %v\n", err)
				resp.HandleBadRequest()
				resp.Write(conn)
				return
			}
		default:
			{
				// Handle request
				resp = s.HandleGoodRequest(req)

				// Write Response
				err := resp.Write(conn)

				if err != nil {
					return // Close on write?
				}

				// Close conn if requested
				if req.Close {
					return
				}
			}
		}
	}
}

// HandleGoodRequest handles the valid req and generates the corresponding res.
func (s *Server) HandleGoodRequest(req *Request) (res *Response) {
	res = &Response{
		Proto: "HTTP/1.1",
		Header: map[string]string{
			"Date": FormatTime(time.Now()),
		},
	}

	// If close given, always close
	if req.Close {
		res.Header["Connection"] = "close"
	}

	cleanedURL := path.Clean(req.URL)
	path := fmt.Sprintf("%v%v", s.DocRoot, cleanedURL)

	stat, err := os.Stat(path)

	if err == nil && stat.IsDir() {
		if !strings.HasSuffix(path, "/") {
			path += "/"
		}

		path += "index.html" // Default root
		fmt.Printf("[Info] Pointing to Directory, appending index.html: %v\n", path)
		stat, err = os.Stat(path) // Refetch info
	}

	if os.IsNotExist(err) {
		res.HandleNotFound(req)
		return res
	}

	if err != nil {
		panic(err) // Should be 500
	}

	// File info
	res.Header["Last-Modified"] = FormatTime(stat.ModTime())
	res.Header["Content-Type"] = MIMETypeByExtension(filepath.Ext(path))
	res.Header["Content-Length"] = fmt.Sprintf("%v", stat.Size())

	res.HandleOK(req, path)
	return res
}

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK(req *Request, path string) {
	res.StatusCode = 200
	res.FilePath = path
}

// HandleBadRequest prepares res to be a 400 Bad Request response
// ready to be written back to client.
func (res *Response) HandleBadRequest() {
	res.StatusCode = 400
	res.Header["Connection"] = "close"
}

// HandleNotFound prepares res to be a 404 Not Found response
// ready to be written back to client.
func (res *Response) HandleNotFound(req *Request) {
	res.StatusCode = 404
}
