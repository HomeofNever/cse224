package surfstore

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

var blockStoreAddr string
var localMeta *map[string]*[]string
var indexMeta *map[string]*FileMetaData
var remoteMeta *map[string]*FileMetaData
var downloadFile, localModifiedFile, remoteModifiedFile, localDeletedFile, newUploadFile []string

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// First, scan the base directory, and for each file, compute that file's hash list
	log.Printf("Client Block Size %v", client.BlockSize)
	log.Printf("Client Basedir %v", client.BaseDir)
	log.Printf("Client Metastore Addr %v", client.MetaStoreAddr)
	localMeta = scanLocalFiles(client.BaseDir, client.BlockSize)

	makeConfigDir(&client.BaseDir)
	indexMetax, err := LoadMetaFromMetaFile(client.BaseDir)
	check(err)
	indexMeta = &indexMetax
	log.Printf("index meta size %v", len(*indexMeta))

	// Prepare remote meta, always use it as source of truth
	updateRemoteMeta(&client)

	downloadFile, localModifiedFile, remoteModifiedFile, localDeletedFile, newUploadFile = make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0)

	// For each of the remote file, see if it exist in the local directory
	for fileName, remoteMetaData := range *remoteMeta {
		indexMetaData, indexOk := (*indexMeta)[fileName]
		localMetaData, localOk := (*localMeta)[fileName]

		if !indexOk && !localOk {
			// Not in local and index, download it
			log.Printf("File should be downloaded since no index or local %v", fileName)
			downloadFile = append(downloadFile, fileName)
			continue
		}

		if !indexOk && localOk {
			// It is not in the index but in the local, compare file status
			if !isEqualHash(&remoteMetaData.BlockHashList, localMetaData) {
				// They are not the same, need update from the server
				// Ignore any from local
				log.Printf("File should be downloaded since not in index but local %v", fileName)
				downloadFile = append(downloadFile, fileName)
			} // or they are the same, skipped
			continue
		}

		if indexOk && !localOk {
			// In the index but not local, we need to compare version and see if we could delete it
			if indexMetaData.Version < remoteMetaData.Version {
				// OOps we cannot delete it, and we need to re-download it
				if !isEqualHash(&remoteMetaData.BlockHashList, &[]string{"0"}) {
					log.Printf("File is deleted and should be downloaded %v", fileName)
					downloadFile = append(downloadFile, fileName)
				} // This needs to be deleted and local has it deleted already, skipped
			} else if indexMetaData.Version == remoteMetaData.Version {
				if !isEqualHash(&indexMetaData.BlockHashList, &[]string{"0"}) {
					// We could update it by delete it
					log.Printf("File should be deleted from server %v", fileName)
					localDeletedFile = append(localDeletedFile, fileName)
				}
			} else {
				// Our index file is out of sync :( fatal!
				log.Fatalf("unexpected local index version comparison for file %v: Client index has Version %v while server has %v", fileName, indexMetaData.Version, remoteMetaData.Version)
			}
			continue
		}

		if indexOk && localOk {
			// Both index and local hold the record, check version first
			if indexMetaData.Version < remoteMetaData.Version {
				// We have left behind, should catch up with server
				log.Printf("File should be downloaded modified %v: %v < %v", fileName, indexMetaData.Version, remoteMetaData.Version)
				remoteModifiedFile = append(remoteModifiedFile, fileName)
			} else if indexMetaData.Version == remoteMetaData.Version {
				// We are at the same version! But, local changes?
				if !isEqualHash(&indexMetaData.BlockHashList, localMetaData) {
					// We have some local update to do :)
					log.Printf("File should be uploaded modified %v", fileName)
					localModifiedFile = append(localModifiedFile, fileName)
				}
			} // Ignore any other unexpected situation for now :|
		}
	}

	// Check from the local perspective
	for fileName := range *localMeta {
		// because we have check remote status before
		// ignore any existing file but look at non-existing one
		if _, ok := (*remoteMeta)[fileName]; !ok {
			// This is a local new file, add to queue
			log.Printf("File should be uploaded %v", fileName)
			newUploadFile = append(newUploadFile, fileName)
		}
	}

	// Now, let's rock!
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	log.Printf("BlockStoreAddr %v", blockStoreAddr)
	check(err)

	// First, let's sync our changes first
	uploadChanges(&client)

	// After upload, it is possible that some of our updates were rejected with newer version
	// Potentially new files if we created one, which does not contains in the current remote hashList
	// So we need to pull the latest hashMap and do the download
	// Even though there maybe more file to be downloaded, we will ignore it until next sync
	updateRemoteMeta(&client)

	// Okay, we need to do some shopping from the block storage
	downloadChanges(&client)

	// Write down our shopping list :)
	// It should always be the remote copy
	log.Print("Begin write meta")
	err = WriteMetaFile(*remoteMeta, client.BaseDir)
	check(err)
}

func updateRemoteMeta(client *RPCClient) {
	// client should connect to the server and download an updated FileInfoMap
	remoteMeta = &map[string]*FileMetaData{}
	err := client.GetFileInfoMap(remoteMeta)
	check(err)
}

func downloadChanges(client *RPCClient) {
	// We don't need to have the latest changes, use the current one to sync
	// For both remoteModified file and downloadFile, do override
	log.Print("Begin remoteModified File")
	for _, fileName := range remoteModifiedFile {
		hashList := (*remoteMeta)[fileName].BlockHashList

		if len(hashList) == 1 && hashList[0] == "0" {
			// this file is marked deleted on remote server
			// delete local file
			log.Printf("File %v should be deleted locally", fileName)
			filePath := getAbsPath(client.BaseDir, fileName)
			err := os.Remove(filePath)
			check(err)
		} else {
			log.Printf("File %v should be override", fileName)
			downloadFileToDestination(fileName, client)
		}
	}

	log.Print("Begin download File")
	for _, fileName := range downloadFile {
		log.Printf("File %v should be download", fileName)
		downloadFileToDestination(fileName, client)
	}
}

func downloadFileToDestination(fileName string, client *RPCClient) {
	filePath := getAbsPath(client.BaseDir, fileName)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	check(err)

	hashList := (*remoteMeta)[fileName].BlockHashList

	for _, hs := range hashList {
		bk := &Block{}
		err := client.GetBlock(hs, blockStoreAddr, bk)
		check(err)

		_, err = f.Write(bk.BlockData)
		check(err)
	}

	check(f.Close())
}

func uploadChanges(client *RPCClient) {
	seekSize := int64(client.BlockSize)
	log.Print("Begin localModified File")
	for _, fileName := range localModifiedFile {
		// find and generate any affected blocks
		log.Printf("Upload Modified file %v", fileName)
		filePath := getAbsPath(client.BaseDir, fileName)
		f, err := os.Open(filePath)
		check(err)

		var existingHash []string
		localHash := *((*localMeta)[fileName])
		// We assume that hasBlocks will return existing block in sequence
		err = client.HasBlocks(localHash, blockStoreAddr, &existingHash)
		check(err)

		existingHashIndex := 0 // Existing Hash should be same or shorter, no checks and assume it for now

		for _, lh := range localHash {
			if existingHashIndex >= len(existingHash) || existingHash[existingHashIndex] != lh {
				// new content, unconditionally upload
				// or upload this non-existing hash
				blockData := make([]byte, client.BlockSize)
				readed, err := f.Read(blockData)
				if err != io.EOF {
					check(err) // EOF is fine
				}
				// Build the Block
				var success bool
				client.PutBlock(&Block{
					BlockData: blockData[:readed],
					BlockSize: int32(readed),
				}, blockStoreAddr, &success)

				if !success {
					log.Fatal("unable to put block: server return flag false")
				}
			} else {
				if existingHash[existingHashIndex] == lh {
					// This hash have already upload, add offset
					existingHashIndex += 1
				}
				f.Seek(seekSize, 1)
			}
		}

		localVersion := (*indexMeta)[fileName].Version
		res := updateFile(fileName, &localHash, localVersion, client)
		if !res {
			log.Printf("Remote server rejected our meta update, queued to download changes: %v", fileName)
			remoteModifiedFile = append(remoteModifiedFile, fileName)
		}
	}

	// Then, we need to upload file that does not exist
	log.Print("Begin newUploadFile")
	for _, fileName := range newUploadFile {
		// find and generate any affected blocks
		filePath := getAbsPath(client.BaseDir, fileName)
		log.Printf("Uploading file %v", filePath)
		f, err := os.Open(filePath)
		check(err)

		bd := make([]byte, client.BlockSize)

		for {
			readed, err := f.Read(bd)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)
			}
			var success bool
			client.PutBlock(&Block{
				BlockData: bd[:readed],
				BlockSize: int32(readed),
			}, blockStoreAddr, &success)

			if !success {
				log.Fatal("unable to put block: server return flag false")
			}
		}

		localHash := *((*localMeta)[fileName])
		res := updateFile(fileName, &localHash, 0, client)
		if !res {
			log.Printf("Remote server rejected our meta update, queued to download changes: %v", fileName)
			remoteModifiedFile = append(remoteModifiedFile, fileName)
		}
	}

	// Then, we need to mark deleted file, if necessary
	log.Print("Begin localDeletedFile")
	for _, fileName := range localDeletedFile {
		localVersion := (*indexMeta)[fileName].Version
		hashList := []string{"0"}
		res := updateFile(fileName, &hashList, localVersion, client)

		if !res {
			log.Printf("Remote server rejected our deletion, queued to download changes: %v", fileName)
			downloadFile = append(downloadFile, fileName)
		}
	}
}

func updateFile(fileName string, hash *[]string, originVersion int32, client *RPCClient) bool {
	var latestVer int32

	err := client.UpdateFile(&FileMetaData{
		Filename:      fileName,
		Version:       originVersion + 1,
		BlockHashList: *hash,
	}, &latestVer)

	log.Printf("Update File return result %v with original Version %v", latestVer, originVersion)
	if err == nil {
		return true
	}

	// We cannot compare latestVer >= originVersion+1 because if err is not nil
	// gRPC does not allow result
	// we will just assume things go wrong and let download handle it
	log.Print(err)
	return false
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func makeConfigDir(baseDir *string) {
	configDir := getAbsPath(*baseDir, DEFAULT_META_FILENAME)

	// client should then consult the local index file and compare the results see whether
	// Check if index.txt exist, if not, create an empty one
	if _, err := os.Stat(configDir); errors.Is(err, os.ErrNotExist) {
		f, err := os.OpenFile(configDir, os.O_RDONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal(err)
		}
		f.Close()
	}
}

func getAbsPath(baseDir string, fileDir string) string {
	path, _ := filepath.Abs(ConcatPath(baseDir, fileDir))
	return path
}

func scanLocalFiles(baseDir string, blockSize int) *map[string]*[]string {
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Fatal(err)
	}

	localMap := map[string]*[]string{}

	for _, file := range files {
		if !file.IsDir() && file.Name() != DEFAULT_META_FILENAME {
			localMap[file.Name()] = getFileHashList(getAbsPath(baseDir, file.Name()), blockSize, (int(file.Size())/blockSize)+1)
		}
	}

	return &localMap
}

// preAlloc give rough size of hash list, 0 if not certain
func getFileHashList(path string, blockSize int, preAlloc int) *[]string {
	f, err := os.Open(path)
	check(err)

	ls := make([]string, 0, preAlloc)
	b1 := make([]byte, blockSize)

	for {
		readed, err := f.Read(b1)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		hash := GetBlockHashString(b1[:readed])
		ls = append(ls, hash)
	}

	return &ls
}

func isEqualHash(a *[]string, b *[]string) bool {
	if len(*a) != len(*b) {
		return false
	}

	for i, v := range *a {
		if v != (*b)[i] {
			return false
		}
	}

	return true
}
