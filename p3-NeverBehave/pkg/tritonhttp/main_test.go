package tritonhttp

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

// Global test setup.
// See https://pkg.go.dev/testing#hdr-Main
func TestMain(m *testing.M) {
	// Suppress logging for all tests in this package
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}
