package surfstore

import (
	"bufio"
	"fmt"
	"net"

	//	"google.golang.org/grpc"
	"io"
	"log"

	//	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("[Client] Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	isCrashedMutex := &sync.RWMutex{}

	log.SetPrefix(fmt.Sprintf("[Server %v]", id))
	server := RaftSurfstore{
		/* -- init new var -- */
		servers:          ips,
		id:               id,
		commitIndex:      -1,
		lastApplied:      -1,
		shouldReachCount: len(ips) / 2,

		isLeader:       false,
		term:           0,
		metaStore:      NewMetaStore(blockStoreAddr),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		notCrashedCond: sync.NewCond(isCrashedMutex),
		isCrashedMutex: isCrashedMutex,
	}

	log.Printf("Server fact: COUNT(%v), numServer(%v)", server.shouldReachCount, len(ips))

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	s := grpc.NewServer()

	RegisterRaftSurfstoreServer(s, server)
	sip := server.servers[server.id]
	log.Printf("Start Serving: %v", sip)

	l, err := net.Listen("tcp", sip)

	if err != nil {
		log.Print(err)
		return err
	}

	return s.Serve(l)
}
