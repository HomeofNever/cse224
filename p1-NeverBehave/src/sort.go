package main

import (
	"io"
	"log"
	"os"
	"sort"
)

type Pair struct {
	key   []byte
	value []byte
}

type byKey []Pair

func (a byKey) Len() int { return len(a) }
func (a byKey) Less(i, j int) bool {
	x, y := a[i].key, a[j].key
	for n := 0; n < len(x); n++ {
		if x[n] < y[n] {
			return true
		} else if x[n] > y[n] {
			return false
		}
	}

	return true
}
func (a byKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}

	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])

	f, err := os.Open(os.Args[1])

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	defer f.Close()

	buf := make([]byte, 100)
	var values byKey

	for {
		_, err = f.Read(buf)

		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
				os.Exit(1)
			}
			break
		}

		key, value := make([]byte, 10), make([]byte, 90)
		copy(key, buf[:9])
		copy(value, buf[10:])

		values = append(values, Pair{
			key,
			value,
		})
	}

	sort.Sort(values)

	err = toFile(values, os.Args[2])

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

func toFile(values byKey, file string) error {
	f, err := os.Create(file)

	if err != nil {
		return err
	}

	defer f.Close()

	for _, v := range values {
		f.Write(v.key)
		f.Write(v.value)
	}

	return nil
}
