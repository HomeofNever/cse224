package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"log"
	sync "sync"
)

var blockMapLock = sync.RWMutex{}

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	blockMapLock.RLock()
	defer blockMapLock.RUnlock()

	hash := blockHash.GetHash()
	// log.Printf("Client is trying to fetch hash %v", hash)
	if bk, ok := bs.BlockMap[hash]; ok {
		// log.Printf("Retrieved hash Success %v", hash)
		return bk, nil
	}

	log.Printf("Retrieved hash not exist %v", hash)
	return nil, errors.New("unable to find block with given hash")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	if block.GetBlockSize() != int32(len(block.GetBlockData())) {
		return &Success{Flag: false}, fmt.Errorf("Block size and data mismatch, sent %v but got %v", len(block.GetBlockData()), block.GetBlockSize())
	}

	blockMapLock.Lock()
	defer blockMapLock.Unlock()

	hash := GetBlockHashString(block.GetBlockData())
	// log.Printf("Client is trying to put block %v, size %v", hash, block.GetBlockSize())
	bs.BlockMap[hash] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes "in", returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashes := make([]string, 0)
	blockMapLock.RLock()
	defer blockMapLock.RUnlock()

	reqHash := blockHashesIn.GetHashes()
	log.Printf("Client is trying to ask for hasBlock with len %v", len(reqHash))
	for _, requiredHash := range reqHash {
		if _, ok := bs.BlockMap[requiredHash]; ok {
			hashes = append(hashes, requiredHash)
		}
	}
	log.Printf("Client hasBlock response len %v", len(hashes))

	return &BlockHashes{
		Hashes: hashes,
	}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
