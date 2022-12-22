package surfstore

import (
	context "context"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.PutBlock(ctx, block)

	if err != nil {
		conn.Close()
		return err
	}

	*succ = b.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.HasBlocks(ctx, &BlockHashes{
		Hashes: blockHashesIn,
	})

	if err != nil {
		conn.Close()
		return err
	}

	*blockHashesOut = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	for _, server := range surfClient.MetaStoreAddrs {
		log.Printf("Try Getting FileinfoMap at %v", server)
		conn, err := grpc.Dial(server, grpc.WithInsecure())
		if err != nil {
			conn.Close()
			continue
		}

		c := NewRaftSurfstoreClient(conn)

		// Assign any value before exit
		b, err := c.GetFileInfoMap(context.Background(), &emptypb.Empty{})

		if err != nil {
			log.Print(err)
			conn.Close()
			if !shouldErrorRetry(err) {
				log.Print("Err should not retry, return.")
				return err
			}
		} else {
			*serverFileInfoMap = b.FileInfoMap

			log.Printf("Remote Server FileInfoMap Size: %v", len(b.FileInfoMap))

			conn.Close()

			return nil
		}
	}

	return ERR_SERVER_CRASHED // We can't find any available
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, server := range surfClient.MetaStoreAddrs {
		log.Printf("Try Updating File at %v", server)
		conn, err := grpc.Dial(server, grpc.WithInsecure())
		if err != nil {
			log.Printf("Connecting err: %v, going next", server)
			conn.Close()
			continue
		}

		c := NewRaftSurfstoreClient(conn)

		// Assign any value before exit
		log.Print("Connected, try gRPC")
		b, err := c.UpdateFile(context.Background(), fileMetaData)

		if err != nil {
			log.Print(err)
			conn.Close()
			if !shouldErrorRetry(err) {
				return err
			}
		} else {
			*latestVersion = b.Version
			conn.Close()
			log.Print("UpdateFile exit successfully.")

			return nil
		}
	}

	return ERR_SERVER_CRASHED // We can't find any available
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server
	for _, server := range surfClient.MetaStoreAddrs {
		log.Printf("Try Getting MetaStoreAddrs at %v", server)
		conn, err := grpc.Dial(server, grpc.WithInsecure())
		if err != nil {
			conn.Close()
			continue
		}

		c := NewRaftSurfstoreClient(conn)
		b, err := c.GetBlockStoreAddr(context.Background(), &emptypb.Empty{})

		if err != nil {
			log.Print(err)
			conn.Close()
			if !shouldErrorRetry(err) {
				return err
			}
		} else {
			*blockStoreAddr = b.Addr
			conn.Close()
			return nil
		}
	}
	return ERR_SERVER_CRASHED // We can't find any available
}

func shouldErrorRetry(err error) bool {
	return err.Error() == "rpc error: code = Unknown desc = "+ERR_NOT_LEADER.Error() || err.Error() == "rpc error: code = Unknown desc = "+ERR_SERVER_CRASHED.Error()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
