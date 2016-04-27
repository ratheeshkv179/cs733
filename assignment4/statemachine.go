package raft

import (
	"errors"
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	"time"
)

var eventChannel = make(chan interface{}, 5)  // all events  timeout, vote req.reply, append req/rply pushed here
var actionChannel = make(chan interface{}, 5) // response events from server machine pushed here
var clientChannel = make(chan interface{}, 5) // append data, commit pushed here

type VoteReqEv struct {
	term         int   // Candidate's term
	candidateId  int   // candidte server ID
	lastLogIndex int64 // candidate last log entry index
	lastLogTerm  int   // candidate last log entry term
}

type VoteRespEv struct {
	term        int  // currentTerm of server who generate this
	voteGranted bool // Vote granted or Not
}

type Entry struct {
	Term    int
	Command string
}

type Timeout struct {
}

type Append struct {
	data string
}

type Commit struct {
	index int64
	data  []Entry
	err   string
}

type AppendEntriesReqEv struct {
	term         int     // leader's term
	leaderId     int     // leader's Id
	prevLogIndex int64   // index of log entry immediately preceding new ones
	prevLogTerm  int     // Term of log entry immediately preceding new ones
	logEntries   []Entry // array of entries to followers (Zero size for heartbeats)
	//	lengthOfLogEnty int64   // length of log entry sending
	leaderCommit int64 // leader's commit index
}

type AppendEntriesRespEv struct {
	id        int   // ServerID who generated this response
	term      int   // CurrentTerm of server who generated this response
	success   bool  // AppendEntriesReqEv is Success or Failure
	lastIndex int64 // last index successfully replicated
}

type StateMachine struct {
	id       int    // this server id
	leaderId int    // leader id of the cluster
	state    string // state of stateMachine LEADER, CANDIDATE or FOLLOWER
	peers    []int  // other server ids

	currentTerm  int         // Latest term server has seen (initially 0, persistent state)
	votedFor     map[int]int // Candidate Id, who received vote for currentTerm (persistent state)
	log          *log.Log    // log of Server     (persistent state)
	commitIndex  int64       // index of highest log entry known to be committed (initial 0, volatile)
	lastApplied  int         // index of highest log entry applied to SM (volatile)
	nextIndex    []int64     // index of next log entry send to that server (volatile on leader, initial leader's last log index+1)
	matchedIndex []int64     // Index of highest log entry known to be replicated (volatile on leader, initial 0)

	voteCount      []int
	replicateCount int
	logLastIndex   int64 // last index in log
	logLastTerm    int   // last term in log

	eventChannel  chan interface{} //= make(chan interface{},5)
	actionChannel chan interface{} //= make(chan interface{},5)
	clientChannel chan interface{} //= make (chan interface{},5)
	// etc
}

func PutEventToChannel(ev interface{}) {
	//eventChannel <- ev
	switch ev.(type) {
	case Commit:
		clientChannel <- ev
	default:
		//node.eventCh <- ev
		eventChannel <- ev
	}
}

func (sm *StateMachine) ProcessEvent(node *RaftNode, ev interface{}) {

	//	//fmt.Print("\nSM PROCESS EVENT :[", node.nodeId, "] Ev : ", ev)

	fmt.Print("")

	switch ev.(type) {

	case Append:

		cmd := ev.(Append)

		if sm.state == "LEADER" {

			////fmt.Print("\nAPPEND", cmd)

			_event := Entry{
				Term:    sm.currentTerm,
				Command: cmd.data}

			err := node.sm.log.Append(_event)

			if err == nil {

				event := AppendEntriesReqEv{
					term:         sm.currentTerm,
					leaderId:     sm.id,
					leaderCommit: sm.commitIndex}

				_prev := sm.log.GetLastIndex()
				event.prevLogIndex = _prev - 1
				val, err := node.sm.log.Get(event.prevLogIndex)

				if err == nil {
					event.prevLogTerm = (val.(Entry)).Term
					event.logEntries = append(event.logEntries, _event)
					for _peer := range node.peers {
						//						//fmt.Print("\nSending To : ", node.peers[_peer], event.logEntries)
						GlobalMsgId = GlobalMsgId + 1
						//						//fmt.Print("\nAppend To :", node.peers[_peer], " Log:", event.logEntries)
						node.server.Outbox() <- &cluster.Envelope{Pid: node.peers[_peer], MsgId: GlobalMsgId, Msg: event}
					}

					//					node.nodeTimer.Stop()
					//					node.nodeTimer.Reset(node.heartbeatTimeout * time.Millisecond)
					//					startNewTimer(node, node.nodeTimer)

					//--------------	stopHeartBeatTimer(node)
					//--------------	node.heartbeatTimer = getTimer(node.heartbeatTimeout)
					//--------------	startTimer(node, node.heartbeatTimer)

				}

				/*for _peer := range node.peers { // NEED TO CHECK "Should exclude it's id"

					if sm.nextIndex[node.peers[_peer]]-1 == 0 {
						event.prevLogIndex = 0
						event.prevLogTerm = 0
					} else {

						_prev, err := sm.log.Get(sm.nextIndex[node.peers[_peer]] - 1)
						if err == nil {
							_prevE := _prev.(Entry)
							event.prevLogIndex = sm.nextIndex[node.peers[_peer]] - 1
							event.prevLogTerm = _prevE.Term
						}
					}
					//fmt.Print("\nIndex {", node.peers[_peer], sm.nextIndex[node.peers[_peer]], " = ", sm.log.GetLastIndex(), "}")
					event.logEntries = []Entry{}
					for i := sm.nextIndex[node.peers[_peer]]; i <= sm.log.GetLastIndex(); i++ {
						val, err := node.sm.log.Get(i)
						if err == nil {
							event.logEntries = append(event.logEntries, val.(Entry))
						}
					}

					//fmt.Print("\n New Entry [", node.nodeId, "] = ", event.logEntries)
					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: node.peers[_peer], MsgId: GlobalMsgId, Msg: event}
				}*/

			} else {
				//	//fmt.Print("\nAppend Error : ", err)
				node.commitCh <- CommitInfo{Data: nil,
					Index: -1,
					err:   errors.New("Append Error")}
			}

		} else if sm.state == "FOLLOWER" {

			if sm.leaderId != -1 { // know the leaderID
				GlobalMsgId = GlobalMsgId + 1
				node.server.Outbox() <- &cluster.Envelope{Pid: sm.leaderId, MsgId: GlobalMsgId, Msg: cmd} // forward to the leader

			} else {
				node.commitCh <- CommitInfo{Data: nil,
					Index: -1,
					err:   errors.New("Don't know the leader")}
			}

		} else if sm.state == "CANDIDATE" {

			node.commitCh <- CommitInfo{Data: nil,
				Index: -1,
				err:   errors.New("Don't know the leader")}
		}
		break

	case AppendEntriesReqEv:

		cmd := ev.(AppendEntriesReqEv)
		//		fmt.Print("\nAppReQ Me:", node.nodeId, " - From:", cmd.leaderId, " - Log:", cmd.logEntries, " -Term:", cmd.term)

		if sm.state == "LEADER" {

			if cmd.term < sm.currentTerm {

				//	//fmt.Print("\n1AppendEntry")

				if len(cmd.logEntries) != 0 { // stale message request, drop it
					//	//fmt.Print("\n1STALE MESSAGE!!!")
					// Drop message, no action
					//return false
					event := AppendEntriesRespEv{
						id:      sm.id,
						term:    sm.currentTerm,
						success: false}

					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

				} else {
					// heart beats from oldterm leader
				}
			} else if cmd.term > sm.currentTerm {

				sm.currentTerm = cmd.term  // Update the current term
				sm.leaderId = cmd.leaderId // update the leaderId
				sm.state = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				if cmd.leaderCommit > sm.commitIndex { // Update my commit index
					my_last_index := sm.log.GetLastIndex()

					if cmd.leaderCommit < my_last_index {
						sm.commitIndex = cmd.leaderCommit
					} else {
						sm.commitIndex = my_last_index
					}
				}

				// ------------	stopHeartBeatTimer(node)
				// ------------	node.electionTimer = getTimer(node.electionTimeout)
				// ------------	startTimer(node, node.electionTimer)

				//	//fmt.Print("\n1LEADER --> FOLLOWER", node.nodeId, " - ", sm.currentTerm, " - ", cmd.leaderId, " - ", cmd.term)

				if len(cmd.logEntries) == 0 { // heartbeat messages

					//			//fmt.Print("\n2AppendEntry")

					//	//fmt.Print("\n1HeartBeat Me:", node.nodeId, "  L:", cmd.leaderId, " # ", time.Now())

					// restart the election timer
					// update my commit index based on leaderCommit field value
					// stopElectionTimer(node)
					// startTimer(node, node.electionTimer)
					if cmd.leaderCommit > sm.commitIndex { // check for current term
						sm.commitIndex = cmd.leaderCommit
						// Apply each to SM
					} else {
					}
					// response to heartbeat message
				} else {

					//		//fmt.Print("\n3AppendEntry")

					_last_index := sm.log.GetLastIndex()
					if cmd.prevLogIndex > _last_index { // No entry in my log corresponding to leader's
						event := AppendEntriesRespEv{
							id:        sm.id,
							term:      sm.currentTerm,
							lastIndex: sm.log.GetLastIndex(),
							success:   false}
						GlobalMsgId = GlobalMsgId + 1
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

					} else { // my log is atleast as lengthy as leader's log

						val, err := sm.log.Get(cmd.prevLogIndex)
						_term := 0
						if err == nil {
							_entry := val.(Entry)
							_term = _entry.Term
						}

						if _term == cmd.prevLogTerm { // matching

							sm.log.TruncateToEnd(cmd.prevLogIndex + 1) // truncate not matching entries
							for i := 0; i < len(cmd.logEntries); i++ {
								sm.log.Append(cmd.logEntries[i]) //
							}

							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   true}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

						} else {

							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   false}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}
						}
					}

				}

			} else {
				// There is atmost one leader per term (this case won't happen)
				//		//fmt.Print("\nSECOND LEADER ON SAME TERM")
			}
			//

		} else if sm.state == "CANDIDATE" {

			if cmd.term < sm.currentTerm {

				//			//fmt.Print("\n4AppendEntry")
				//
				if len(cmd.logEntries) != 0 { // stale message request, drop it
					//		//fmt.Print("\n2STALE MESSAGE!!!")
					// Drop message, no action
					//return false
					event := AppendEntriesRespEv{
						id:      sm.id,
						term:    sm.currentTerm,
						success: false}

					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

				} else {
					// heart beats from oldterm leader
				}
			} else {

				sm.currentTerm = cmd.term // Update the current term
				sm.state = "FOLLOWER"
				sm.leaderId = cmd.leaderId // update the leaderId

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				//-------------	stopElectionTimer(node)
				//-------------	node.electionTimer = getTimer(node.electionTimeout)
				//-------------	startTimer(node, node.electionTimer)

				//				stopElectionTimer(node)
				//				node.electionTimer = getTimer(node.electionTimeout)
				//				startTimer(node, node.electionTimer)

				if len(cmd.logEntries) == 0 { // heartbeat messages

					//		//fmt.Print("\n5AppendEntry")

					////fmt.Print("\n2HeartBeat Me:", node.nodeId, "  L:", cmd.leaderId, " # ", time.Now())

					//			//fmt.Print("\n1HEARTBEAT ", sm.state, " - ", cmd.leaderId, " - ", node.nodeId)
					// restart the election timer
					// update my commit index based on leaderCommit field value
					//				stopElectionTimer(node)
					//				startTimer(node, node.electionTimer)

					// response to heartbeat message

				} else { // Actual AppendEntryReqRPC event

					//		//fmt.Print("\n6AppendEntry")

					_last_index := sm.log.GetLastIndex()
					if cmd.prevLogIndex > _last_index { // No entry in my log corresponding to leader's
						event := AppendEntriesRespEv{
							id:        sm.id,
							term:      sm.currentTerm,
							lastIndex: sm.log.GetLastIndex(),
							success:   false}
						GlobalMsgId = GlobalMsgId + 1
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

					} else { // my log is atleast as lengthy as leader's log

						val, err := sm.log.Get(cmd.prevLogIndex)
						_term := 0
						if err == nil {
							_entry := val.(Entry)
							_term = _entry.Term
						}

						if _term == cmd.prevLogTerm { // matching

							sm.log.TruncateToEnd(cmd.prevLogIndex + 1) // truncate not matching entries
							for i := 0; i < len(cmd.logEntries); i++ {
								sm.log.Append(cmd.logEntries[i]) //
							}

							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   true}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

						} else {

							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   false}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}
						}
					}

				}

			}

		} else if sm.state == "FOLLOWER" {

			////fmt.Print("\nAppReQ Me:", node.nodeId, " - From:", cmd.leaderId, " - Log:", cmd.logEntries, " -Term:", cmd.term)

			if cmd.term < sm.currentTerm {

				//	//fmt.Print("\n7AppendEntry")

				if len(cmd.logEntries) != 0 { // stale message request, drop it
					//	//fmt.Print("\n3STALE MESSAGE!!!")
					// Drop message, no action
					//return false
					event := AppendEntriesRespEv{
						id:      sm.id,
						term:    sm.currentTerm,
						success: false}

					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

				} else {
					// heart beats from oldterm leader
				}
			} else { // CHECK "for same term:will it be a leader"

				sm.currentTerm = cmd.term // Update the current term
				sm.state = "FOLLOWER"
				sm.leaderId = cmd.leaderId // update the leaderId

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				if cmd.leaderCommit > sm.commitIndex { // Update my commit index
					my_last_index := sm.log.GetLastIndex()

					if cmd.leaderCommit < my_last_index {
						sm.commitIndex = cmd.leaderCommit
					} else {
						sm.commitIndex = my_last_index
					}
				}

				//-------------	stopElectionTimer(node)
				//-------------	node.electionTimer = getTimer(node.electionTimeout)
				//-------------	startTimer(node, node.electionTimer)

				if len(cmd.logEntries) == 0 { // heartbeat messages

					//					//fmt.Print("\n8AppendEntry")

					//					//fmt.Print("\n3HeartBeat Me:", node.nodeId, "  L:", cmd.leaderId, " # ", time.Now())

					//				//fmt.Print("\n2HEARTBEAT ", sm.state, " - ", cmd.leaderId, " - ", node.nodeId)

					// restart the election timer
					// update my commit index based on leaderCommit field value

					// response to heartbeat message
				} else { // Actual AppendEntryReqRPC event

					//		//fmt.Print("\n9AppendEntry")
					//
					_last_index := sm.log.GetLastIndex()

					// //fmt.Print("\n MyIndex[", _last_index, "] leaderIndex[", cmd.prevLogIndex, "]")

					if cmd.prevLogIndex > _last_index { // No entry in my log corresponding to leader's

						event := AppendEntriesRespEv{
							id:        sm.id,
							term:      sm.currentTerm,
							lastIndex: sm.log.GetLastIndex(),
							success:   false}
						GlobalMsgId = GlobalMsgId + 1
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

						//			//fmt.Print("\nONE - AppendEntry")

					} else { // my log is atleast as lengthy as leader's log

						val, err := sm.log.Get(cmd.prevLogIndex)
						_term := 0
						if err == nil {
							_entry := val.(Entry)
							_term = _entry.Term
						}

						if _term == cmd.prevLogTerm { // matching
							////fmt.Print("\n\nMatching ", node.nodeId, " ")

							sm.log.TruncateToEnd(cmd.prevLogIndex + 1) // truncate not matching entries
							if len(cmd.logEntries) > 8 {
								//fmt.Println("***#***", time.Now())

							}
							for i := 0; i < len(cmd.logEntries); i++ {
								////fmt.Print("\n[", sm.id, "] APPEND [", sm.log.GetLastIndex(), "]", cmd.logEntries[i])
								if i%2 == 0 {
									node.nodeTimer.Stop()
									node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
									startNewTimer(node, node.nodeTimer)
								}
								sm.log.Append(cmd.logEntries[i]) //
							}
							if len(cmd.logEntries) > 8 {
								//fmt.Println("******", time.Now())

							}

							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   true}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}
							//				//fmt.Print("\nTWO - AppendEntry ", cmd.leaderId)

						} else {
							//fmt.Print("\n\nNot Matching ", node.nodeId, " ")

							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   false}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}
							//				//fmt.Print("\nTHREE - AppendEntry ", cmd.leaderId)
						}
					}
				}
				// CHECK for the following (not sure)

			}
		}

		break

	case AppendEntriesRespEv:

		//		//fmt.Print("\n$AppendResp")

		cmd := ev.(AppendEntriesRespEv)
		//		//fmt.Print("\nAppRes", node.nodeId, " - ", cmd.id, " - ", cmd.success, " - ", cmd.lastIndex)

		if cmd.term < sm.currentTerm { // Ignore the message packet...

			//			//fmt.Print("\n0AppendResp")
			//
		} else if cmd.term > sm.currentTerm { // Higher term

			//		//fmt.Print("\n6AppendResp")
			///
			if sm.state == "LEADER" {

				//	f/mt.Print("\n1AppendResp")

				//				//fmt.Print("\n2LEADER --> FOLLOWER")

				//fmt.Print("\n2LEADER --> FOLLOWER", node.nodeId, " - ", sm.currentTerm, " - ", cmd.id, " - ", cmd.term)

				sm.state = "FOLLOWER" //

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				//-------------	stopHeartBeatTimer(node)
				//-------------	node.electionTimer = getTimer(node.electionTimeout)
				//-------------	startTimer(node, node.electionTimer)
			} else if sm.state == "CANDIDATE" {

				//	//fmt.Print("\n2AppendResp")

				sm.state = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				//------------- stopElectionTimer(node)
				//------------- node.electionTimer = getTimer(node.electionTimeout)
				//------------- startTimer(node, node.electionTimer)
			}
			sm.currentTerm = cmd.term

		} else { // same term

			//			//fmt.Print("\n3AppendResp")

			if sm.state == "LEADER" {

				if cmd.success { // The Entry is successfully replicated in the server

					//				//fmt.Print("\n4AppendResp")

					sm.matchedIndex[cmd.id] = cmd.lastIndex //sm.log.GetLastIndex()

					if sm.commitIndex < cmd.lastIndex {

						for i := node.sm.commitIndex + 1; i <= cmd.lastIndex; i++ { //  Updation of commit index logic
							count := 0
							for j := 1; j <= 5; j++ {
								if sm.matchedIndex[j] >= i {
									count++
								}
							}
							if count >= 2 {
								val, err := sm.log.Get(i)
								if err == nil {
									_val := val.(Entry)
									if _val.Term == sm.currentTerm { // Leader will commit entries of it's own term
										node.sm.commitIndex++

									}
								}
							}
						}
					}

					/*
						_replicas := 0
						// commit for majority
						for _, val := range sm.matchedIndex { // count the number of successful replications
							//if val == cmd.lastIndex {
							if val >= (sm.commitIndex + 1) {
								_replicas++
							}
						}
						if _replicas >= 3 && sm.commitIndex < cmd.lastIndex { // replicated in majority of servers

							event := CommitInfo{Index: sm.logLastIndex, err: nil}
							event.Data = make([]Entry, cmd.lastIndex-sm.nextIndex[cmd.id]+1)
							j := 0
							for i := sm.nextIndex[cmd.id]; i <= cmd.lastIndex; i++ {
								val, err := sm.log.Get(i)
								////fmt.Print("\nERROR :::", err)
								if err == nil {
									val1 := val.(Entry)
									event.Data[j] = val1
								}
								j++
							}
							node.commitCh <- event
							sm.commitIndex = cmd.lastIndex
						}*/

					sm.nextIndex[cmd.id] = cmd.lastIndex + 1 //sm.log.GetLastIndex()+1

				} else { // log inConsistency, retry AppenedEntryRPC request

					//				//fmt.Print("\n5AppendResp")

					sm.nextIndex[cmd.id] = sm.nextIndex[cmd.id] - 1

					if sm.nextIndex[cmd.id] > 0 {

						val, _ := sm.log.Get(sm.nextIndex[cmd.id] - 1)
						val1 := val.(Entry)

						var entries = []Entry{}
						j := 0

						for i := sm.nextIndex[cmd.id]; i <= sm.log.GetLastIndex(); i++ {
							//entries[j] = (sm.log.Get(i)).Entry
							val, err := sm.log.Get(i)
							if err == nil {
								_val := val.(Entry)
								entries = append(entries, _val)
							}
							j++
						}

						event := AppendEntriesReqEv{
							term:         sm.currentTerm,
							leaderId:     sm.id,
							prevLogIndex: sm.nextIndex[cmd.id] - 1, //sm.logLastIndex - 1,
							prevLogTerm:  val1.Term,
							logEntries:   entries,
							leaderCommit: sm.commitIndex}
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.id, MsgId: GlobalMsgId, Msg: event}

					} else {
						var entries = []Entry{}
						var i int64
						for i = 1; i <= sm.log.GetLastIndex(); i++ {
							//entries[j] = (sm.log.Get(i)).Entry
							val, err := sm.log.Get(i)
							if err == nil {
								_val := val.(Entry)
								entries = append(entries, _val)
							}
						}
						event := AppendEntriesReqEv{
							term:         sm.currentTerm,
							leaderId:     sm.id,
							prevLogIndex: 0, //sm.logLastIndex - 1,
							prevLogTerm:  0,
							logEntries:   entries,
							//lengthOfLogEnty: 1,
							leaderCommit: sm.commitIndex}
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.id, MsgId: GlobalMsgId, Msg: event}
					}
				}
			}
		}
		break

	case VoteReqEv: //

		cmd := ev.(VoteReqEv)

		//	//fmt.Print("\nVOTE REQ from ", cmd.candidateId, " - Me ", node.nodeId, " - ", sm.state)

		if sm.state == "LEADER" {
			//		//fmt.Print("\n@")

			val, err := sm.log.Get(sm.log.GetLastIndex())

			//		//fmt.Print("\n$ ", cmd.candidateId, " - ", cmd.lastLogTerm, " = ", (val.(Entry)).Term, " = ", cmd.lastLogIndex, " = ", sm.log.GetLastIndex(), " - ", node.nodeId)

			if err == nil {

				if cmd.lastLogTerm > (val.(Entry)).Term { //sm.currentTerm { // check for upto date

					//	//fmt.Print("\n#1")

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}

					}

				} else if cmd.lastLogTerm == (val.(Entry)).Term && cmd.lastLogIndex >= sm.log.GetLastIndex() { // check for upto date

					//			//fmt.Print("\n#2")
					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
					}

				} else {
					//				//fmt.Print("\n#3")
					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
				}
			}

			if cmd.term > sm.currentTerm {
				//				//fmt.Print("\n3LEADER --> FOLLOWER", node.nodeId, " - ", cmd.candidateId)

				//fmt.Print("\n3LEADER --> FOLLOWER", node.nodeId, " - ", sm.currentTerm, " - ", cmd.candidateId, " - ", cmd.term)

				sm.currentTerm = cmd.term
				sm.state = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				//------------- stopHeartBeatTimer(node)
				//------------- node.electionTimer = getTimer(node.electionTimeout)
				//------------- startTimer(node, node.electionTimer)
			}

		} else if sm.state == "CANDIDATE" {

			val, err := sm.log.Get(sm.log.GetLastIndex())

			//		//fmt.Print("\n# ", cmd.candidateId, " - ", cmd.lastLogTerm, " = ", (val.(Entry)).Term, " = ", cmd.lastLogIndex, " = ", sm.log.GetLastIndex(), " - ", node.nodeId)

			if err == nil {

				if cmd.lastLogTerm > (val.(Entry)).Term { //sm.currentTerm { // check for upto date

					//		//fmt.Print("\n#1")
					//
					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}

					}

				} else if cmd.lastLogTerm == (val.(Entry)).Term && cmd.lastLogIndex >= sm.log.GetLastIndex() { // check for upto date

					//		//fmt.Print("\n#2")
					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
					}

				} else {
					//		//fmt.Print("\n#3")
					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
				}
			}

			if cmd.term > sm.currentTerm {
				sm.currentTerm = cmd.term
				sm.state = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				//------------- stopElectionTimer(node)
				//------------- node.electionTimer = getTimer(node.electionTimeout)
				//------------- startTimer(node, node.electionTimer)
			}

		} else if sm.state == "FOLLOWER" {

			//		//fmt.Print("\nVOTE ", sm.currentTerm, sm.log.GetLastIndex(), sm.votedFor[cmd.term], " - FROM ", cmd)

			//

			//			//fmt.Print("\n* ", cmd.candidateId, " - ", cmd.lastLogTerm, " = ", sm.currentTerm, " = ", cmd.lastLogIndex, " = ", sm.log.GetLastIndex(), " - ", node.nodeId)

			val, err := sm.log.Get(sm.log.GetLastIndex())

			//		//fmt.Print("\n@ ", cmd.candidateId, " - ", cmd.lastLogTerm, " = ", (val.(Entry)).Term, " = ", cmd.lastLogIndex, " = ", sm.log.GetLastIndex(), " - ", node.nodeId)

			if err == nil {

				if cmd.lastLogTerm > (val.(Entry)).Term { //sm.currentTerm { // check for upto date

					//		//fmt.Print("\n#1")

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}

					}

				} else if cmd.lastLogTerm == (val.(Entry)).Term && cmd.lastLogIndex >= sm.log.GetLastIndex() { // check for upto date

					//			//fmt.Print("\n#2")
					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
					}

				} else {
					//		//fmt.Print("\n#3")
					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
				}
			}

			if cmd.term > sm.currentTerm {
				sm.currentTerm = cmd.term
			}

		}

		break

	case VoteRespEv:

		////fmt.Print("VoteRespEv Mesage")

		cmd := ev.(VoteRespEv)

		////fmt.Print("\nVOTE RES ", cmd.voteGranted, " - ", sm.currentTerm, " - ", cmd.term, " - ", node.nodeId, " - ", sm.state)

		if sm.state == "LEADER" {

			if cmd.term > sm.currentTerm { // new leader/ leader with higher term exist
				////fmt.Print("\n4LEADER --> FOLLOWER", node.nodeId, " - ", sm.currentTerm, " - ", cmd.term)
				sm.currentTerm = cmd.term
				sm.state = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				//------------- stopHeartBeatTimer(node)
				//------------- node.electionTimer = getTimer(node.electionTimeout)
				//------------- startTimer(node, node.electionTimer)
			}
		} else if sm.state == "CANDIDATE" {

			////fmt.Print("\nVOTE RES ", cmd.voteGranted, " - ", cmd.term, " - ", sm.currentTerm, " - ", node.nodeId)

			/*if cmd.term > sm.currentTerm {

				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term

				stopElectionTimer(node)
				node.electionTimer = getTimer(node.electionTimeout)
				startTimer(node, node.electionTimer)
			} else if cmd.term < sm.currentTerm { // Stale response message, Ignore
				//
			} else { */

			if cmd.voteGranted && (sm.currentTerm == cmd.term) {

				//				sm.voteCount[sm.currentTerm] = sm.voteCount[sm.currentTerm] + 1 // increment votecount
				sm.voteCount[0] = sm.voteCount[0] + 1 // increment votecount
				////fmt.Print("\nVOTE SUCCESS COUNT : ", sm.voteCount[0])

				if sm.voteCount[0] == 3 {

					////fmt.Print("\nVOTE SUCCESS ", node.nodeId)
					sm.voteCount[0] = 0
					sm.voteCount[1] = 0

					sm.state = "LEADER"
					sm.leaderId = sm.id

					event := AppendEntriesReqEv{
						term:     sm.currentTerm,
						leaderId: sm.id,
						//							prevLogIndex:    sm.logLastIndex,
						//							prevLogTerm:     val1.term,
						//							lengthOfLogEnty: 0,
						// logEntries:   []Entry{},
						leaderCommit: sm.commitIndex}

					for _peer := range node.peers {

						event.logEntries = []Entry{}
						for i := sm.nextIndex[node.peers[_peer]]; i <= sm.log.GetLastIndex(); i++ {
							event.prevLogIndex = sm.nextIndex[node.peers[_peer]] - 1
							_entry, err := sm.log.Get(event.prevLogIndex)
							event.prevLogTerm = 0
							if err == nil {
								__entry := _entry.(Entry)
								event.prevLogTerm = __entry.Term
							}
							val, err := node.sm.log.Get(i)
							if err == nil {
								event.logEntries = append(event.logEntries, val.(Entry))
							}
						}

						//fmt.Print("\n New Entry [", node.peers[_peer], " - ", sm.nextIndex[node.peers[_peer]], "] = ", event.logEntries)
						GlobalMsgId = GlobalMsgId + 1
						node.server.Outbox() <- &cluster.Envelope{Pid: node.peers[_peer], MsgId: GlobalMsgId, Msg: event}
					}
					node.nodeTimer.Stop()
					node.nodeTimer.Reset(node.heartbeatTimeout * time.Millisecond)
					startNewTimer(node, node.nodeTimer)
					//----------------- stopElectionTimer(node)
					//----------------- node.heartbeatTimer = getTimer(node.heartbeatTimeout)
					//----------------- startTimer(node, node.heartbeatTimer)

				}
			} else if !cmd.voteGranted {

				sm.voteCount[1] = sm.voteCount[1] + 1 // increment votecount

				if cmd.term > sm.currentTerm {
					sm.state = "FOLLOWER"
					sm.currentTerm = cmd.term

					node.nodeTimer.Stop()
					node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
					startNewTimer(node, node.nodeTimer)

					//----------------  stopElectionTimer(node)
					//----------------  node.electionTimer = getTimer(node.electionTimeout)
					//----------------  startTimer(node, node.electionTimer)

				} else {

					if sm.voteCount[1] == 3 {

						//		//fmt.Print("\nVOTE SUCCESS ")

						sm.state = "FOLLOWER"
						sm.leaderId = sm.id

						node.nodeTimer.Stop()
						node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
						startNewTimer(node, node.nodeTimer)

						//--------------------- stopElectionTimer(node)
						//--------------------- node.electionTimer = getTimer(node.electionTimeout)
						//--------------------- startTimer(node, node.electionTimer)

						/*event := AppendEntriesReqEv{
							term:     sm.currentTerm,
							leaderId: sm.id,
							//							prevLogIndex:    sm.logLastIndex,
							//							prevLogTerm:     val1.term,
							//							lengthOfLogEnty: 0,
							logEntries:   []Entry{},
							leaderCommit: sm.commitIndex}
						node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}
						*/
					}

				}
			}
			//}
		} else if sm.state == "FOLLOWER" {

			if cmd.term > sm.currentTerm { // update the term
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term
			}
		}

		break

	case Timeout:

		if sm.state == "LEADER" {

			event := AppendEntriesReqEv{
				term:     sm.currentTerm,
				leaderId: sm.id,
				//				prevLogIndex:    sm.logLastIndex,
				//				prevLogTerm:     val1.term,
				logEntries:   []Entry{},
				leaderCommit: sm.commitIndex}

			GlobalMsgId++
			//			node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}

			for _peer := range node.peers {
				//						//fmt.Print("\nSending To : ", node.peers[_peer], event.logEntries)
				//	GlobalMsgId = GlobalMsgId + 1
				node.server.Outbox() <- &cluster.Envelope{Pid: node.peers[_peer], MsgId: GlobalMsgId, Msg: event}
			}

			node.nodeTimer.Stop()
			node.nodeTimer.Reset(node.heartbeatTimeout * time.Millisecond)
			startNewTimer(node, node.nodeTimer)

			//--------- stopHeartBeatTimer(node)
			//--------- node.heartbeatTimer = getTimer(node.heartbeatTimeout)
			//--------- startTimer(node, node.heartbeatTimer)

		} else if sm.state == "CANDIDATE" { // Election timeout, starts next-election
			//			//fmt.Print("\nTIMEOUT2 ", node.nodeId, " - ", time.Now())
			//	//fmt.Print("\nTIMEOUT CANDIDATE ", node.nodeId, time.Now())

			sm.currentTerm = sm.currentTerm + 1 // increment the term
			sm.votedFor[sm.currentTerm] = sm.id // voted himself
			sm.voteCount[0] = 1                 // voted himself
			sm.voteCount[1] = 0
			sm.leaderId = -1 // I don't know the leader

			indx := sm.log.GetLastIndex()
			_entry, _ := sm.log.Get(indx)
			__entry := _entry.(Entry)

			event := VoteReqEv{
				term:         sm.currentTerm,
				candidateId:  sm.id,
				lastLogIndex: indx,
				lastLogTerm:  __entry.Term}

			GlobalMsgId = GlobalMsgId + 1
			node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}

			node.nodeTimer.Stop()
			node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
			startNewTimer(node, node.nodeTimer)

			//---------- stopElectionTimer(node)
			//----------node.electionTimer = getTimer(node.electionTimeout)
			//----------startTimer(node, node.electionTimer)

		} else if sm.state == "FOLLOWER" { // Election Timeout, start election
			//	//fmt.Print("\nTIMEOUT FOLLOWER ", node.nodeId, time.Now())
			//			//fmt.Print("\nTIMEOUT3 ", node.nodeId, " - ", time.Now())
			sm.currentTerm = sm.currentTerm + 1 // increment the term
			sm.state = "CANDIDATE"              // change to CANDIDATE state
			sm.votedFor[sm.currentTerm] = sm.id // voted himself
			sm.voteCount[0] = 1                 // voted himself
			sm.voteCount[1] = 0

			//--------- stopElectionTimer(node)
			//--------- node.electionTimer = getTimer(node.electionTimeout)
			//--------- startTimer(node, node.electionTimer)

			sm.leaderId = -1 // I don't know the leader

			indx := sm.log.GetLastIndex()
			_entry, _ := sm.log.Get(indx)
			__entry := _entry.(Entry)

			event := VoteReqEv{
				term:         sm.currentTerm,
				candidateId:  sm.id,
				lastLogIndex: indx,
				lastLogTerm:  __entry.Term}

			GlobalMsgId = GlobalMsgId + 1
			node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}

			node.nodeTimer.Stop()
			node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
			startNewTimer(node, node.nodeTimer)

		}
	}
}
