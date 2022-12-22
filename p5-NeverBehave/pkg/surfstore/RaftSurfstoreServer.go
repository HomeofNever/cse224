package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	/* --- added locker var ---*/
	servers []string
	id      int64

	nextIndex  []int64
	matchIndex []int64

	commitIndex      int64
	lastApplied      int64
	shouldReachCount int

	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	for {
		log.Print("Request incoming: GetFileInfoMap")
		// perform the call
		req, err := s.SendHeartbeat(ctx, empty)

		if err != nil {
			log.Print(err)
			return nil, err
		}

		if req.Flag {
			log.Print("Flag Return true while Get Fileinfo")
			return s.metaStore.GetFileInfoMap(ctx, empty)
		}

		log.Print("GetFileInfo: Looping...")
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	err := s.isWorkingLeader()
	if err != nil {
		return nil, err
	}

	return s.metaStore.GetBlockStoreAddr(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	err := s.isWorkingLeader()
	if err != nil {
		log.Print(err)
		return nil, err
	}

	s.isCrashedMutex.Lock()
	log.Print("Update file: adding to queue")
	// Start update queue
	str := make([]string, len(filemeta.BlockHashList))
	for i := range str {
		str[i] = filemeta.BlockHashList[i]
	}
	op := UpdateOperation{
		Term: s.term,
		FileMetaData: &FileMetaData{
			Filename:      filemeta.Filename,
			Version:       filemeta.Version,
			BlockHashList: str,
		},
	}

	s.log = append(s.log, &op)
	log.Printf("Updated s.log %v", s.log)

	commitChan := make(chan *AppendEntryOutput, len(s.servers)-1)
	for idx := range s.servers {
		idx64 := int64(idx)
		// Don't send to yourself
		if idx64 != s.id {
			go s.commitWorker(idx64, commitChan, true)
		}
	}
	s.isCrashedMutex.Unlock()

	// There are commit to be commit, making space
	commitCount := 1 // Local has record
	success := false
	for {
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		if commitCount > s.shouldReachCount {
			success = true
			break
		} // Or we wait indefinitely
	}

	if success {
		s.isCrashedMutex.Lock()
		s.commitIndex += 1
		s.isCrashedMutex.Unlock()

		for {
			// Waiting for commit
			if s.SendHeartBeatCommit() {
				break
			}
		}

		// update the lastApplied
		s.isCrashedMutex.Lock()
		defer s.isCrashedMutex.Unlock()
		s.lastApplied = s.commitIndex

		return s.metaStore.UpdateFile(ctx, filemeta)
	} else {
		return nil, fmt.Errorf("ERR_UNKNOWN")
	}
}

// This should always retry, until success
// Or, you are no longer in the place to update
func (s *RaftSurfstore) commitWorker(serverIdx int64, commitChan chan *AppendEntryOutput, shouldRetry bool) {
	for {
		// Am I the leader? If not, just return don't retry
		err := s.isWorkingLeader()
		if err != nil {
			log.Print(err)
			commitChan <- &AppendEntryOutput{
				Success: false,
			}
			return
		}

		// set up clients or call grpc.Dial
		conn, err := grpc.Dial(s.servers[serverIdx], grpc.WithInsecure())
		if err != nil {
			fmt.Print(err)
			continue // Go back and retry
		}

		client := NewRaftSurfstoreClient(conn)

		// create correct AppendEntryInput from s.nextIndex, etc
		s.isCrashedMutex.RLock()
		nxt := s.nextIndex[serverIdx]
		prevIndex := nxt - 1
		mth := s.matchIndex[serverIdx]
		var prevTerm int64 = 0 // If we don't have a ground truth, we don't care the term
		if prevIndex > -1 {
			prevTerm = s.log[prevIndex].Term
		}

		input := &AppendEntryInput{
			Term:         s.term,
			LeaderCommit: s.commitIndex,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
		}

		if mth == -2 {
			// if matchIndex is -1, then we are still looking for the right start point
			// don't generate the log yet
			input.Entries = make([]*UpdateOperation, 0)
		} else {
			input.Entries = s.log[nxt:len(s.log)]
		}

		log.Printf("Init Pack: SRV(%v), Term(%v), LC(%v), PLI(%v), PLT(%v), NXT(%v), MTH(%v), LOG(%v)", serverIdx, s.term, s.commitIndex, prevIndex, prevTerm, nxt, mth, len(input.Entries))
		log.Print(input.Entries)

		s.isCrashedMutex.RUnlock()

		output, err := client.AppendEntries(context.Background(), input)

		if err != nil {
			if err.Error() == "rpc error: code = Unknown desc = Server is crashed." {
				if !shouldRetry {
					// Don't retry Crashed, heartbeat will make it up
					log.Print("Give up retry because it is crashed")
					commitChan <- &AppendEntryOutput{
						Success: false,
					}
					return
				} else {
					time.Sleep(time.Second) // Sleep to avoid CTX timeout
				}
			}
			continue
		}

		// If this is not the probe
		if mth != -2 && output.Success {
			log.Printf("Sending to chan %v, CLEN(%v)", output, len(commitChan))
			commitChan <- output

			log.Printf("Send Success for server %v", serverIdx)
			s.isCrashedMutex.Lock()
			s.nextIndex[serverIdx] = output.MatchedIndex + 1
			s.matchIndex[serverIdx] = output.MatchedIndex
			s.isCrashedMutex.Unlock()
			return
		} else if output.Success {
			// We find the right index, go back and construct
			log.Printf("Send Success for heartbeat MIX(%v)", output.MatchedIndex)
			s.isCrashedMutex.Lock()
			s.matchIndex[serverIdx] = output.MatchedIndex
			s.nextIndex[serverIdx] = output.MatchedIndex + 1
			s.isCrashedMutex.Unlock()
		} else {
			// else continue, try again with different variables
			log.Printf("Send Failed for server %v", serverIdx)
			s.isCrashedMutex.Lock()
			s.nextIndex[serverIdx]--
			s.isCrashedMutex.Unlock()
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn't contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	log.Print("AE Request Incoming")
	s.isCrashedMutex.Lock()
	defer s.isCrashedMutex.Unlock()

	if s.isCrashed {
		log.Print("AE Request: SRC")
		return nil, ERR_SERVER_CRASHED
	}

	log.Printf("Incoming: PLI(%v), PLT(%v), SLOG(%v), LD(%v)", input.PrevLogIndex, input.PrevLogTerm, len(input.Entries), input.LeaderCommit)

	output := &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		MatchedIndex: -1,
		Success:      false,
	}

	// term mismatch
	if input.Term < s.term {
		log.Printf("Term mismatch: self(%v), incoming(%v)", s.term, input.Term)
		return output, nil
	} else if input.Term > s.term {
		log.Printf("No longer leader: self(%v), incoming(%v)", s.term, input.Term)
		s.term = input.Term
		s.isLeader = false // No longer leader
	}

	// log mismatch
	prevLogIndex := input.PrevLogIndex
	prevLogTerm := input.PrevLogTerm
	// No such index
	if len(s.log)-1 < int(prevLogIndex) {
		log.Printf("ERR: not such index PLI(%v), SLOG(%v)", prevLogIndex, len(s.log))
		return output, nil
	}

	if prevLogIndex < -1 {
		return nil, fmt.Errorf("ERR_ILLEGAL_PREVLOGINDEX: %v", prevLogIndex)
	}

	if prevLogIndex != -1 && s.log[prevLogIndex].Term != prevLogTerm {
		log.Printf("ERR: Mismatch PLI(%v), TERM(%v), incoming(%v)", prevLogIndex, s.log[prevLogIndex].Term, prevLogTerm)
		return output, nil
	}

	// -1 means we don't have a ground truth, must accept it

	// Prev Log Index matched, update current
	// Starting from prevLog
	newLogLen := len(input.Entries)
	if newLogLen == 0 {
		// No new Entries, this probably a heartbeat
		log.Printf("Heartbeat Received")
		// If server is asking from ground truth then we should clear all current logs, start fresh
		if prevLogIndex == -1 {
			s.log = make([]*UpdateOperation, 0)
		}
	} else {
		// Has new entries
		startIdx := int(prevLogIndex + 1)
		newLogIdx := 0
		log.Printf("New Entries received, startIDX(%v)", startIdx)

		// Potential conflict, check first
		if len(s.log)-int(prevLogIndex)-1 > 0 {
			log.Printf("Potential Conflict detected, SLOG(%v)", len(s.log))
			for startIdx < len(s.log) && newLogIdx < len(input.Entries) {
				if s.log[startIdx].Term != input.Entries[newLogIdx].Term {
					// Mismatch, delete all items starting here
					origin := s.log[:startIdx]
					s.log = make([]*UpdateOperation, startIdx)
					copy(s.log, origin)
					break
				}
				startIdx++
				newLogIdx++
			}
		}

		// Append from newLogIdx
		log.Printf("Appending entries from %v to %v", newLogIdx, len(input.Entries))
		s.log = append(s.log, input.Entries[newLogIdx:len(input.Entries)]...)
		log.Print(s.log)
	}

	// Check commitIndex
	localLongest := int64(len(s.log) - 1)
	if s.commitIndex < input.LeaderCommit {
		// What's the farthest commit we could do?
		// If we don't have that many logs, we commit to the furthest point and set to that
		if localLongest < input.LeaderCommit {
			s.commitIndex = localLongest
		} else {
			// We have, do the leader one
			s.commitIndex = input.LeaderCommit
		}
	}

	if s.lastApplied < s.commitIndex {
		for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
			s.metaStore.UpdateFile(context.Background(), s.log[i].FileMetaData)
		}
		s.lastApplied = s.commitIndex
	}

	output.Success = true
	output.MatchedIndex = localLongest
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	defer s.isCrashedMutex.Unlock()

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if s.isLeader {
		// We are already leader, no need to set
		return &Success{Flag: true}, nil
	}

	s.isLeader = true
	s.term += 1

	s.nextIndex = make([]int64, len(s.servers))
	s.matchIndex = make([]int64, len(s.servers))

	lastLogIndex := int64(len(s.log))
	for i := range s.nextIndex {
		s.nextIndex[i] = lastLogIndex
		s.matchIndex[i] = -2
	}

	// No need to send heartbeat ourselves, SendHeartbeat will be called right after this method

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.isWorkingLeader(); err != nil {
		return &Success{Flag: false}, err
	}

	op := s.buildLatestHeartbeat()
	num := len(s.servers) - 1
	heartBeatChan := make(chan int, num)
	wg := sync.WaitGroup{}
	wg.Add(num)

	for idx, _ := range s.servers {
		idx64 := int64(idx)
		// Don't send to yourself
		if idx64 != s.id {
			go s.sendHeartBeat(idx64, op, &heartBeatChan, &wg)
		}
	}

	log.Print("HB: waiting for reply")
	wg.Wait()
	close(heartBeatChan)

	healthy := 1 // Local has record
	crashed := 0
	for result := range heartBeatChan {
		if result == -1 {
			crashed++
		} else if result == 1 {
			healthy++
		}
	}

	// Look for any node that left behind
	dispatch := 0
	commitChan := make(chan *AppendEntryOutput, num)
	s.isCrashedMutex.RLock()
	for idx := range s.servers {
		idx64 := int64(idx)
		if idx64 != s.id && (int(s.nextIndex[idx]) < len(s.log) || s.commitIndex > s.nextIndex[idx] || int(s.matchIndex[idx]) < len(s.log)-1) {
			go s.commitWorker(idx64, commitChan, false)
			log.Printf("Index need to update: %v", idx64)
			dispatch += 1
		}
	}
	s.isCrashedMutex.RUnlock()

	for i := 0; i < dispatch; i++ {
		log.Printf("HB: waiting index(%v), num(%v)", i, num)
		<-commitChan
	}

	log.Print("HB: wait done, result calculation")

	if crashed > s.shouldReachCount {
		log.Printf("More Crash(%v) than ShouldReach(%v)", crashed, s.shouldReachCount)
		return &Success{
			Flag: false,
		}, nil
	}

	if healthy <= s.shouldReachCount {
		log.Printf("Less Healthy(%v) than ShouldReach(%v)", healthy, s.shouldReachCount)
		// Go back retry, node is healthy and need to be updated
		return s.SendHeartbeat(ctx, &emptypb.Empty{})
	}

	return &Success{
		Flag: true,
	}, nil
}

func (s *RaftSurfstore) SendHeartBeatCommit() bool {
	if err := s.isWorkingLeader(); err != nil {
		return false
	}

	op := s.buildLatestHeartbeat()
	num := len(s.servers) - 1
	heartBeatChan := make(chan int, num)
	wg := sync.WaitGroup{}
	wg.Add(num)

	for idx, _ := range s.servers {
		idx64 := int64(idx)
		// Don't send to yourself
		if idx64 != s.id {
			go s.sendHeartBeat(idx64, op, &heartBeatChan, &wg)
		}
	}

	log.Print("HB: waiting for reply")
	wg.Wait()
	close(heartBeatChan)

	healthy := 1 // Local has record
	for result := range heartBeatChan {
		if result == 1 {
			healthy++
		}
	}

	return healthy >= (len(s.servers) / 2)
}

// -1 unable to connect
// 0 connected, but rejected
// 1 connected, accepted
func (s *RaftSurfstore) sendHeartBeat(serverIdx int64, op *AppendEntryInput, request *chan int, wg *sync.WaitGroup) {
	defer wg.Done()

	// set up clients or call grpc.Dial
	conn, err := grpc.Dial(s.servers[serverIdx], grpc.WithInsecure())
	if err != nil {
		log.Print(err)
		*request <- -1
		return
	}

	client := NewRaftSurfstoreClient(conn)

	// create correct AppendEntryInput from s.nextIndex, etc
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	output, err := client.AppendEntries(ctx, op)

	defer cancel()

	if err != nil {
		log.Print(err)
		*request <- -1
		return
	}

	if output.Success {
		*request <- 1
	} else {
		*request <- 0
	}
}

func (s *RaftSurfstore) buildLatestHeartbeat() *AppendEntryInput {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	idx := len(s.log) - 1
	op := &AppendEntryInput{
		Term:         s.term,
		Entries:      make([]*UpdateOperation, 0), // empty for heartbeat info
		LeaderCommit: s.commitIndex,
		PrevLogIndex: int64(idx),
	}

	if idx < 0 {
		op.PrevLogTerm = 0
	} else {
		op.PrevLogTerm = s.log[idx].Term
	}

	return op
}

func (s *RaftSurfstore) isWorkingLeader() error {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return ERR_NOT_LEADER
	}

	return nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
