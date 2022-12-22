package surfstore

import (
	context "context"
	"fmt"
	"log"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

var fileMetaMapLock = sync.RWMutex{}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fileMetaMapLock.RLock()
	defer fileMetaMapLock.RUnlock()
	log.Printf("Client is asking for fileInfoMap, current size %v", len(m.FileMetaMap))

	// We need to copy the map to avoid any Further writes from the other components
	copyMap := map[string]*FileMetaData{}

	for k, v := range m.FileMetaMap {
		copyMap[k] = &FileMetaData{
			Filename:      v.Filename,
			Version:       v.Version,
			BlockHashList: v.BlockHashList, // Assume hash list will only be replaced instead of update
		}
	}

	return &FileInfoMap{FileInfoMap: copyMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileMetaMapLock.Lock()
	defer fileMetaMapLock.Unlock()

	fileName := fileMetaData.GetFilename()
	uploadedVersion := fileMetaData.GetVersion()
	log.Printf("[Info] Client is uploading file %v with version %v", fileName, uploadedVersion)
	if file, ok := m.FileMetaMap[fileName]; ok {
		// File Exist
		currentVersion := file.Version
		if currentVersion+1 == uploadedVersion {
			// Update file
			m.FileMetaMap[fileName].BlockHashList = fileMetaData.BlockHashList
			m.FileMetaMap[fileName].Version = uploadedVersion
			log.Printf("[Success] Client is uploading file %v with version %v", fileName, uploadedVersion)
			return &Version{
				Version: uploadedVersion,
			}, nil
		}

		log.Printf("[Failed] Client is uploading file %v with version %v, server Version %v", fileName, uploadedVersion, currentVersion)

		return &Version{
			Version: currentVersion,
		}, fmt.Errorf("server has %v version of file %v: but client provide version %v. Exactly +1 version required", currentVersion, fileName, uploadedVersion)

	} else {
		// No related file, we may want to see if we want to create one?
		if uploadedVersion == 1 {
			m.FileMetaMap[fileName] = fileMetaData
			log.Printf("[Success] Client is uploading file %v with version %v, server file created", fileName, uploadedVersion)
			return &Version{
				Version: 1,
			}, nil
		}

		log.Printf("[Failed] Client is uplading file %v with version %v, new file should have version 1", fileName, uploadedVersion)

		return &Version{
			Version: 1,
		}, fmt.Errorf("server does not have given file %v and to create a file, version 1 is required. Client given %v", fileName, uploadedVersion)
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	log.Printf("Client is asking Block Store Addr %v", m.BlockStoreAddr)
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
