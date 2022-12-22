package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

var receivedValues []byKey
var wg sync.WaitGroup
var sendwg sync.WaitGroup

type ServerConfig struct {
	ServerId int    `yaml:"serverId"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
}

type ServerConfigs struct {
	Servers []ServerConfig `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	if err != nil {
		log.Fatal(err)
	}

	return scs
}

func findConfig(config *ServerConfigs, serverId int) *ServerConfig {
	for _, v := range config.Servers {
		if v.ServerId == serverId {
			return &v
		}
	}

	return nil
}

func startSending(config ServerConfig, data *byKey, serverId uint) {
	var c net.Conn = nil

	fmt.Printf("Trying to connect: %v\n", config)

	for i := 0; i < 5; i++ {
		fmt.Printf("Connect attempt: %d\n", i)

		var err error
		c, err = net.Dial("tcp", fmt.Sprintf("%v:%v", config.Host, config.Port))
		if err == nil {
			break
		} else {
			fmt.Println("[ERR] Unable to Connect, Err: ", err)
		}
		time.Sleep(30 * time.Second)
	}

	if c != nil {
		fmt.Printf("Connection established to %s\n", c.RemoteAddr().String())
		defer c.Close()
	} else {
		log.Printf("[ERR] Unable to connect to the server %s. Give up trying sending data\n", config.Host)
		// Do not retry
	}

	// Even though length 0, we need to notified others we don't have any data
	id := uint16(serverId)
	// First, send our Server ID
	err := binary.Write(c, binary.LittleEndian, id)
	if err != nil {
		fmt.Printf("[ERR] %v\n", err)
	}
	fmt.Printf("Sent Server ID: %v\n", id)

	// Second, send our length
	length := uint64(len(*data))
	err = binary.Write(c, binary.LittleEndian, length)
	if err != nil {
		fmt.Printf("[ERR] %v\n", err)
	}
	fmt.Printf("Sent Entry number: %v\n", length)

	// Then we send the data
	for _, v := range *data {
		_, err := c.Write(append(v.key, v.value...))
		if err != nil {
			fmt.Printf("[ERR] %v\n", err)
			return
		}
	}

	// Mark done when finished
	sendwg.Done()
}

func handleRequest(conn net.Conn) {
	fmt.Println("New Connection Accepted.")

	defer conn.Close()
	defer fmt.Println("Connection Closed.")

	// Server Should

	// first indicate its serverID
	// We assume that: Server ID will be unique for every incoming server
	var id uint16
	err := binary.Read(conn, binary.LittleEndian, &id)
	fmt.Printf("Incoming Server ID: %v\n", id)

	// If any fails drop connection
	if err != nil {
		log.Fatal(err) // We don't like err :(
	}

	// Second indicate how many entry to send
	var num uint64
	err = binary.Read(conn, binary.LittleEndian, &num)

	fmt.Printf("Number of Entry: %d\n", num)

	// If any fails drop connection
	if err != nil {
		log.Fatal(err) // We don't like err :(
	}

	temp := make(byKey, num)
	var i uint64 = 0

	for ; i < num; i++ {
		buf := make([]byte, 100)
		_, err := io.ReadFull(conn, buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("[ERR] EOF read before all entry received")
				break
			} else if os.IsTimeout(err) {
				fmt.Println("[ERR] Timeout before receiving data")
			}
			return // Drop temp value
		}

		temp[i], err = Byte2Pair(&buf)
		if err != nil {
			fmt.Println("[ERR] Broken Stream received, existed")
			return // Drop Value, await retry
		}
	}

	receivedValues[id] = temp
	// Mark finished
	wg.Done()
}

func startListen(port string) {
	l, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Fatalf("Unable to listen port: %v", port)
	}

	fmt.Println("Server started at port: ", port)

	defer l.Close()
	defer fmt.Println("Server Stopped Listening.")

	for {
		conn, err := l.Accept()

		if err != nil {
			log.Fatal(err)
		}

		go handleRequest(conn)
	}
}

func getServerId(key byte, bit uint) uint {
	move := 8 - bit
	return uint(key >> move)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// @TODO current only 256 server will be supported
	if serverId > 256 {
		log.Fatal("Max 256 Server supported currently")
	}

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])

	fmt.Println("Got the following server configs:", scs)

	// Init Values
	numServer := len(scs.Servers)
	// Sanity Check
	if serverId < 0 && serverId >= numServer {
		log.Fatalf("Invalid serverId, it should between 0 and %v", numServer)
	}
	numBit := uint(math.Log2(float64(numServer))) // maybe better way doing this
	receivedValues = make([]byKey, numServer)
	sendValue := make([]byKey, numServer)
	wg.Add(numServer - 1)
	sendwg.Add(numServer - 1)

	// Read file
	var values byKey
	err = (&values).FromFile(os.Args[2])

	if err != nil {
		log.Fatal(err)
	}

	// Find all keys for self and others
	// We assume that server will be lowered than 256
	for _, v := range values {
		id := getServerId(v.key[0], numBit)
		sendValue[id] = append(sendValue[id], v)
	}

	// Find Current Config
	config := findConfig(&scs, serverId)

	if config == nil {
		log.Fatalf("Unable to find matching server id in config %v", serverId)
	}

	// Start Listen with given port
	port := config.Port
	go startListen(port)

	// Send values to others
	for _, v := range scs.Servers {
		// Don't send to self!
		if v.ServerId != serverId {
			// We don't cares about errors for now :(
			fmt.Printf("Sending data to server %v\n", v.ServerId)
			go startSending(v, &sendValue[v.ServerId], uint(serverId))
		}
	}

	// Wait for others to send values
	wg.Wait()
	fmt.Println("All Server Info Acquired, merging data...")

	// Merge Value into Pool
	var allValues byKey
	for _, v := range receivedValues {
		allValues = append(allValues, v...)
	}
	allValues = append(allValues, sendValue[serverId]...)
	fmt.Printf("Merged, %d pair in total\n", len(allValues))

	// Sort value
	fmt.Println("Sorting...")
	sort.Sort(allValues)
	fmt.Println("Sort Complete")

	// Write Back
	fmt.Println("Writing file...")
	(&allValues).ToFile(os.Args[3])
	fmt.Println("Write finished")

	// We also need to wait send group to be complete
	sendwg.Wait()
	fmt.Println("All Group Sent, exited")
}

// Pair.go

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

// sort.Sort(values)
func (values *byKey) FromFile(file string) error {
	f, err := os.Open(file)

	if err != nil {
		return err
	}

	defer f.Close()

	buf := make([]byte, 100)

	for {
		_, err = f.Read(buf)

		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}

		pair, _ := Byte2Pair(&buf)
		*values = append(*values, pair)
	}

	return nil
}

func (values *byKey) ToFile(file string) error {
	f, err := os.Create(file)

	if values == nil {
		return nil
	}

	if err != nil {
		return err
	}

	defer f.Close()

	for _, v := range *values {
		f.Write(v.key)
		f.Write(v.value)
	}

	return nil
}

func Byte2Pair(stream *[]byte) (Pair, error) {
	l := len(*stream)
	if l != 100 {
		return Pair{}, fmt.Errorf("expected byte length 100, got %v", l)
	}
	key, value := make([]byte, 10), make([]byte, 90)
	copy(key, (*stream)[:10])
	copy(value, (*stream)[10:])

	return Pair{
		key,
		value,
	}, nil
}
