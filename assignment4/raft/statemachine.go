package raft

/**
Ratheesh k v
CS 733
IIT Bombay
*/

import (
	"errors"
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
	//	"github.com/ratheeshkv179/cs733/assignment5/fs"
	"github.com/ratheeshkv179/cs733/assignment5/fs"
	"time"
)

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
	Term    int     // term info
	Command *fs.Msg // command info

}

type Timeout struct { // Time out object for timeout event
}

type Append struct {
	data *fs.Msg // Append information
}

type AppendEntriesReqEv struct {
	term         int     // leader's term
	leaderId     int     // leader's Id
	prevLogIndex int64   // index of log entry immediately preceding new ones
	prevLogTerm  int     // Term of log entry immediately preceding new ones
	logEntries   []Entry // array of entries to followers (Zero size for heartbeats)
	leaderCommit int64   // leader's commit index
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
	State    string // State of StateMachine LEADER, CANDIDATE or FOLLOWER
	peers    []int  // other server ids

	currentTerm  int         // Latest term server has seen (initially 0, persistent State)
	votedFor     map[int]int // Candidate Id, who received vote for currentTerm (persistent State)
	log          *log.Log    // log of Server     (persistent State)
	commitIndex  int64       // index of highest log entry known to be committed (initial 0, volatile)
	lastApplied  int         // index of highest log entry applied to SM (volatile)
	nextIndex    []int64     // index of next log entry send to that server (volatile on leader, initial leader's last log index+1)
	matchedIndex []int64     // Index of highest log entry known to be replicated (volatile on leader, initial 0)

	voteCount []int // indicate number of vote granted for a particular term

}

// State machine process the events

func (sm *StateMachine) ProcessEvent(node *RaftNode, ev interface{}) {

	fmt.Print("")

	switch ev.(type) {

	// APPEND request from client
	case Append:

		cmd := ev.(Append)

		// state is LEADER
		if sm.State == "LEADER" {

			_event := Entry{ // create anentry to append to log
				Term:    sm.currentTerm,
				Command: cmd.data}

			err := node.Sm.log.Append(_event) // Append to leader's log

			if err == nil {

				event := AppendEntriesReqEv{ // AppendEntry request to other clients
					term:         sm.currentTerm, // my term
					leaderId:     sm.id,          // leaderID
					leaderCommit: sm.commitIndex} // committed index

				_prev := sm.log.GetLastIndex()
				event.prevLogIndex = _prev - 1
				val, err := node.Sm.log.Get(event.prevLogIndex)

				if err == nil {
					event.prevLogTerm = (val.(Entry)).Term
					event.logEntries = append(event.logEntries, _event)
					// Send APpend request to each node for replication
					for _peer := range node.peers {
						GlobalMsgId = GlobalMsgId + 1
						node.server.Outbox() <- &cluster.Envelope{Pid: node.peers[_peer], MsgId: GlobalMsgId, Msg: event}
					}

				}

			} else { // Error while appending

				node.commitCh <- CommitInfo{Data: Entry{},
					Index: -1,
					Err:   errors.New("Append Error")}
			}

		} else if sm.State == "FOLLOWER" || sm.State == "CANDIDATE" {

			obj := CommitInfo{Err: errors.New("ERR_REDIRECT")}
			node.commitCh <- obj
		}

		/*if sm.leaderId != -1 { // know the leaderID
				GlobalMsgId = GlobalMsgId + 1
				node.server.Outbox() <- &cluster.Envelope{Pid: sm.leaderId, MsgId: GlobalMsgId, Msg: cmd} // forward to the leader
			} else {

				node.commitCh <- CommitInfo{Data: Entry{},
					Index: -1,
					err:   errors.New("Don't know the leader")}
			}
		} else if sm.State == "CANDIDATE" {
			node.commitCh <- CommitInfo{Data: Entry{},
				Index: -1,
				err:   errors.New("Don't know the leader")}
		}*/

		break

	// Appendentry request from LEADER
	case AppendEntriesReqEv:

		cmd := ev.(AppendEntriesReqEv)

		if sm.State == "LEADER" { // state is leader

			if cmd.term < sm.currentTerm { // my term is higher, s stale message

				if len(cmd.logEntries) != 0 { // Stale old AppendRequet

					event := AppendEntriesRespEv{ // reply false, so that other end update the latest term
						id:      sm.id,
						term:    sm.currentTerm,
						success: false}

					GlobalMsgId = GlobalMsgId + 1
					// send false response
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

				} else {
					// heart beats from oldterm leader
				}
			} else if cmd.term > sm.currentTerm { // receives term is higher

				sm.currentTerm = cmd.term  // Update the current term
				sm.leaderId = cmd.leaderId // update the leaderId
				sm.State = "FOLLOWER"      // step down to follower

				node.nodeTimer.Stop()                                         // Stop heartbeat timeout
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond) // reset election timer
				startNewTimer(node, node.nodeTimer)                           // start election timer

				if cmd.leaderCommit > sm.commitIndex { // Update my commit index
					my_last_index := sm.log.GetLastIndex()
					if cmd.leaderCommit < my_last_index {
						sm.commitIndex = cmd.leaderCommit
					} else {
						sm.commitIndex = my_last_index
					}
				}

				if len(cmd.logEntries) == 0 { // heartbeat messages

					if cmd.leaderCommit > sm.commitIndex { // update commit index
						sm.commitIndex = cmd.leaderCommit
					}

				} else {
					// appendEntry requst for data entry

					_last_index := sm.log.GetLastIndex()
					if cmd.prevLogIndex > _last_index { // No entry in my log corresponding to leader's prevlog index
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

						if _term == cmd.prevLogTerm { // my term and leader's term are matching

							sm.log.TruncateToEnd(cmd.prevLogIndex + 1) // truncate not matching entries
							for i := 0; i < len(cmd.logEntries); i++ {
								sm.log.Append(cmd.logEntries[i]) // Append entries to get consistent with leader
							}

							// send back success , means my log consistent with leader's
							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   true}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

						} else {

							// my log in inconsistent state, reply false

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
			}

		} else if sm.State == "CANDIDATE" { // iam candidate

			if cmd.term < sm.currentTerm { // stale message request, drop it

				if len(cmd.logEntries) != 0 {
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

				sm.currentTerm = cmd.term  // Update the current term
				sm.State = "FOLLOWER"      // step down to follower
				sm.leaderId = cmd.leaderId // update the leaderId

				// reset election timer
				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

				if len(cmd.logEntries) == 0 { // heartbeat messages

				} else { // Actual AppendEntryReqRPC event

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

						if _term == cmd.prevLogTerm { // my log matching with leader's

							sm.log.TruncateToEnd(cmd.prevLogIndex + 1) // truncate not matching entries
							for i := 0; i < len(cmd.logEntries); i++ {
								sm.log.Append(cmd.logEntries[i]) //
							}

							// reply success my log in consistent state

							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   true}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

						} else {

							// reply false Log insconsistency
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

		} else if sm.State == "FOLLOWER" { // i am follower

			if cmd.term < sm.currentTerm { // request with old term

				if len(cmd.logEntries) != 0 { // stale message request, drop it

					event := AppendEntriesRespEv{ // reply false
						id:      sm.id,
						term:    sm.currentTerm,
						success: false}

					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

				} else {
					// heart beats from oldterm leader
				}
			} else { // CHECK "for same term:will it be a leader"

				sm.currentTerm = cmd.term  // Update the current term
				sm.State = "FOLLOWER"      // remains as followe
				sm.leaderId = cmd.leaderId // update the leaderId

				// reset election timer
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

				if len(cmd.logEntries) == 0 { // heartbeat messages

				} else { // Actual AppendEntryReqRPC event

					_last_index := sm.log.GetLastIndex()

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

						if _term == cmd.prevLogTerm { // matching with leader

							sm.log.TruncateToEnd(cmd.prevLogIndex + 1) // truncate not matching entries
							if len(cmd.logEntries) > 8 {

							}
							for i := 0; i < len(cmd.logEntries); i++ {
								if i%2 == 0 {
									// looping take more time, update election timer, other wise times out
									node.nodeTimer.Stop()
									node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
									startNewTimer(node, node.nodeTimer)
								}
								sm.log.Append(cmd.logEntries[i]) // Append to my log
							}

							// reply succsss, my log consistent with leader
							event := AppendEntriesRespEv{
								id:        sm.id,
								term:      sm.currentTerm,
								lastIndex: sm.log.GetLastIndex(),
								success:   true}
							GlobalMsgId = GlobalMsgId + 1
							node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

						} else {

							// log inconsistency , reply false
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
		}

		break

	case AppendEntriesRespEv:

		cmd := ev.(AppendEntriesRespEv)

		if cmd.term < sm.currentTerm { // old term pkt, Ignore the message packet...

			//
		} else if cmd.term > sm.currentTerm { //got msg with higher term

			///
			if sm.State == "LEADER" { // if leader

				sm.State = "FOLLOWER" // step down to follower

				// stop heartbeats,  reset election timer
				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

			} else if sm.State == "CANDIDATE" { //candidate

				sm.State = "FOLLOWER" //step down to follower

				// stop heartbeats,  reset election timer
				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

			}
			sm.currentTerm = cmd.term // update term

		} else { // same term

			if sm.State == "LEADER" {

				if cmd.success { // The Entry is successfully replicated in the server

					sm.matchedIndex[cmd.id] = cmd.lastIndex //update matched index

					if sm.commitIndex < cmd.lastIndex { // update commit index

						for i := node.Sm.commitIndex + 1; i <= cmd.lastIndex; i++ { //  Updation of commit index logic
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
										node.Sm.commitIndex++

										ev, err := node.Sm.log.Get(node.Sm.commitIndex)
										_ev := ev.(Entry)

										// commit, send to commit channel
										if err == nil {
											obj := CommitInfo{
												Data:  _ev,
												Index: node.Sm.commitIndex,
												Err:   nil}
											node.commitCh <- obj
										}

									}
								}
							}
						}
					}

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

		if sm.State == "LEADER" {

			val, err := sm.log.Get(sm.log.GetLastIndex())

			if err == nil {

				if cmd.lastLogTerm > (val.(Entry)).Term { //sm.currentTerm { // check for upto date

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}

					}

				} else if cmd.lastLogTerm == (val.(Entry)).Term && cmd.lastLogIndex >= sm.log.GetLastIndex() { // check for upto date

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
					}

				} else {
					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
				}
			}

			if cmd.term > sm.currentTerm {

				sm.currentTerm = cmd.term
				sm.State = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

			}

		} else if sm.State == "CANDIDATE" {

			val, err := sm.log.Get(sm.log.GetLastIndex())

			if err == nil {

				if cmd.lastLogTerm > (val.(Entry)).Term { //sm.currentTerm { // check for upto date

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
					}

				} else if cmd.lastLogTerm == (val.(Entry)).Term && cmd.lastLogIndex >= sm.log.GetLastIndex() { // check for upto date

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
					}

				} else {
					GlobalMsgId = GlobalMsgId + 1
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
				}
			}

			if cmd.term > sm.currentTerm {
				sm.currentTerm = cmd.term
				sm.State = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

			}

		} else if sm.State == "FOLLOWER" {

			val, err := sm.log.Get(sm.log.GetLastIndex())

			if err == nil {

				if cmd.lastLogTerm > (val.(Entry)).Term { //sm.currentTerm { // check for upto date

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}

					}

				} else if cmd.lastLogTerm == (val.(Entry)).Term && cmd.lastLogIndex >= sm.log.GetLastIndex() { // check for upto date

					GlobalMsgId = GlobalMsgId + 1

					if sm.votedFor[cmd.term] == 0 { // NOt voted for this term yet
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: true}}
						sm.votedFor[cmd.term] = cmd.term
					} else { // already voted
						node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: cmd.term, voteGranted: false}}
					}

				} else {
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

		cmd := ev.(VoteRespEv)

		if sm.State == "LEADER" {

			if cmd.term > sm.currentTerm { // new leader/ leader with higher term exist
				sm.currentTerm = cmd.term
				sm.State = "FOLLOWER"

				node.nodeTimer.Stop()
				node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
				startNewTimer(node, node.nodeTimer)

			}
		} else if sm.State == "CANDIDATE" {

			if cmd.voteGranted && (sm.currentTerm == cmd.term) {

				sm.voteCount[0] = sm.voteCount[0] + 1 // increment votecount

				if sm.voteCount[0] == 3 {

					sm.voteCount[0] = 0
					sm.voteCount[1] = 0

					sm.State = "LEADER"
					sm.leaderId = sm.id

					event := AppendEntriesReqEv{
						term:         sm.currentTerm,
						leaderId:     sm.id,
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
							val, err := node.Sm.log.Get(i)
							if err == nil {
								event.logEntries = append(event.logEntries, val.(Entry))
							}
						}

						GlobalMsgId = GlobalMsgId + 1
						node.server.Outbox() <- &cluster.Envelope{Pid: node.peers[_peer], MsgId: GlobalMsgId, Msg: event}
					}
					node.nodeTimer.Stop()
					node.nodeTimer.Reset(node.heartbeatTimeout * time.Millisecond)
					startNewTimer(node, node.nodeTimer)

				}
			} else if !cmd.voteGranted {

				sm.voteCount[1] = sm.voteCount[1] + 1 // increment votecount

				if cmd.term > sm.currentTerm {
					sm.State = "FOLLOWER"
					sm.currentTerm = cmd.term

					node.nodeTimer.Stop()
					node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
					startNewTimer(node, node.nodeTimer)

				} else {

					if sm.voteCount[1] == 3 {

						sm.State = "FOLLOWER"
						sm.leaderId = sm.id

						node.nodeTimer.Stop()
						node.nodeTimer.Reset(node.electionTimeout * time.Millisecond)
						startNewTimer(node, node.nodeTimer)

					}

				}
			}
			//}
		} else if sm.State == "FOLLOWER" {

			if cmd.term > sm.currentTerm { // update the term
				sm.State = "FOLLOWER"
				sm.currentTerm = cmd.term
			}
		}

		break

	case Timeout:

		if sm.State == "LEADER" {

			event := AppendEntriesReqEv{
				term:         sm.currentTerm,
				leaderId:     sm.id,
				logEntries:   []Entry{},
				leaderCommit: sm.commitIndex}

			GlobalMsgId++

			for _peer := range node.peers {
				node.server.Outbox() <- &cluster.Envelope{Pid: node.peers[_peer], MsgId: GlobalMsgId, Msg: event}
			}

			node.nodeTimer.Stop()
			node.nodeTimer.Reset(node.heartbeatTimeout * time.Millisecond)
			startNewTimer(node, node.nodeTimer)

		} else if sm.State == "CANDIDATE" { // Election timeout, starts next-election

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

		} else if sm.State == "FOLLOWER" { // Election Timeout, start election
			//	//fmt.Print("\nTIMEOUT FOLLOWER ", node.nodeId, time.Now())
			//			//fmt.Print("\nTIMEOUT3 ", node.nodeId, " - ", time.Now())
			sm.currentTerm = sm.currentTerm + 1 // increment the term
			sm.State = "CANDIDATE"              // change to CANDIDATE State
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

		}
	}
}
