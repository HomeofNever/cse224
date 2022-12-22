package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"cse224/proj3/pkg/tritonhttp"
)

func main() {
	// Parse command line flags
	var useDefault = flag.Bool("use_default", false, "whether to use the Golang standard library HTTP server")
	var port = flag.Int("port", 8080, "the localhost port to listen on")
	var docRoot = flag.String("doc_root", "htdocs", "path to the doc root directory")
	flag.Parse()

	// Log server configs
	log.Print("Server configs:")
	log.Printf("  use_default: %v", *useDefault)
	log.Printf("  port: %v", *port)
	log.Printf("  doc_root: %v", *docRoot)

	// Start server
	addr := fmt.Sprintf(":%v", *port)
	if *useDefault {
		log.Printf("Starting default server")
		log.Printf("You can browse the website at http://localhost:%v/", *port)
		s := &http.Server{
			Addr:    addr,
			Handler: http.FileServer(http.Dir(*docRoot)),
		}
		log.Fatal(s.ListenAndServe())
	} else {
		log.Printf("Starting TritonHTTP server")
		log.Printf("You can browse the website at http://localhost:%v/", *port)
		s := &tritonhttp.Server{
			Addr:    addr,
			DocRoot: *docRoot,
		}
		log.Fatal(s.ListenAndServe())
	}
}
