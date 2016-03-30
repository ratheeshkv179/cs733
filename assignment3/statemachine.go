package raft

import (
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/log"
)

var eventChannel = make(chan interface{}, 5)  // all events  timeout, vote req.reply, append req/rply pushed here
var actionChannel = make(chan interface{}, 5) // response events from server machine pushed here
var clientChannel = make(chan interface{}, 5) // append data, commit pushed here

type VoteReqEv struct {
	term         int
	candidateId  int
	lastLogIndex int64
	lastLogTerm  int
}

type VoteRespEv struct {
	term        int
	voteGranted bool
}

type Entry struct {
	term    int
	command string
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
	term            int //
	leaderId        int
	prevLogIndex    int64
	prevLogTerm     int
	logEntries      Entry
	lengthOfLogEnty int64 // length of log entry
	leaderCommit    int64
}

type AppendEntriesRespEv struct {
	id      int  // serverID who generated this response
	term    int  // term numner
	success bool //
}

type StateMachine struct {
	id             int         // this server id
	leaderId       int         // leader id of the cluster
	state          string      // state of stateMachine LEADER, CANDIDATE or FOLLOWER
	peers          []int       // other server ids
	currentTerm    int         // current term
	votedFor       map[int]int //make(map[int]int) // candidateID vs voted or not
	voteCount      [10]int
	replicateCount int
	log            *log.Log // log of state machine
	logLastIndex   int64    // last index in log
	logLastTerm    int      // last term in log
	commitIndex    int64    // latest committed index
	lastApplied    int
	nextIndex      []int64
	matchedIndex   []int64
	eventChannel   chan interface{} //= make(chan interface{},5)
	actionChannel  chan interface{} //= make(chan interface{},5)
	clientChannel  chan interface{} //= make (chan interface{},5)
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

	fmt.Print("\nSM PROCESS EVENT :", node.nodeId, " Ev : ", ev)

	switch ev.(type) {
	case Append:
		cmd := ev.(Append)

		fmt.Print("\nAppend Message")
		if sm.state == "LEADER" {

			err := sm.log.Append(Entry{term: sm.currentTerm, command: cmd.data})
			if err == nil {
				val, _ := sm.log.Get(sm.logLastIndex)
				val1 := val.(Entry)
				event := AppendEntriesReqEv{
					term:            sm.currentTerm,
					leaderId:        sm.id,
					prevLogIndex:    sm.logLastIndex,
					prevLogTerm:     val1.term,
					logEntries:      Entry{term: sm.currentTerm, command: cmd.data}, // New Entry to replicate
					lengthOfLogEnty: 1,
					leaderCommit:    sm.commitIndex}

				sm.logLastIndex = sm.log.GetLastIndex() // last index of log entry
				sm.logLastTerm = sm.currentTerm         // current term

				GlobalMsgId = GlobalMsgId + 1
				node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}
				//				sm.nextIndex
				//				sm.matchedIndex
			} else {
				fmt.Print("\nAppend Error : ", err)
			}

		} else if sm.state == "FOLLOWER" {
			// redirect to leader
			if sm.leaderId != -1 { // know the leaderID
				GlobalMsgId = GlobalMsgId + 1
				node.server.Outbox() <- &cluster.Envelope{Pid: sm.leaderId, MsgId: GlobalMsgId, Msg: cmd} // forward to the leader
			}
		}

	case AppendEntriesReqEv:

		fmt.Print("\nAppendEntriesReqEv Message")

		cmd := ev.(AppendEntriesReqEv)

		// my term is higher
		if cmd.term < sm.currentTerm {
			event := AppendEntriesRespEv{
				term:    sm.currentTerm,
				success: false}
			//   go PutEventToChannel(Send{cmd.leaderId,event})
			GlobalMsgId = GlobalMsgId + 1
			node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}
			break
		}

		if cmd.lengthOfLogEnty == 0 {
			// heart beat from leader
			//refresh the election timeout
			// create an alarm, which Timeout after t milliseconds.
			//event := Alarm{t:200}
			//go PutEventToChannel(event)

			// refresh election timeout value
			// node.electionTimeout
			sm.leaderId = cmd.leaderId
			stopElectionTimer(node)
			node.electionTimer = getTimer(node.electionTimeout)
			startTimer(node, node.electionTimer)
			fmt.Print("\nUpdate Election timeout")

		} else {

			if sm.currentTerm < cmd.term {
				sm.currentTerm = cmd.term
				sm.state = "FOLLOWER"
			}

			// not a heartbeat, it is an actuall append request
			sm.leaderId = cmd.leaderId // got leaderID
			// no entry for particular index, my log is old
			if sm.logLastIndex < cmd.prevLogIndex {
				event := AppendEntriesRespEv{
					id:      sm.id,
					term:    sm.currentTerm,
					success: false}
				//go PutEventToChannel(Send{cmd.leaderId,event})
				node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}
				break
			}

			// no matching entries in log (matching index and term)
			val, _ := sm.log.Get(cmd.prevLogIndex)
			val1 := val.(Entry)

			if (sm.logLastIndex >= cmd.prevLogIndex) && (val1.term != cmd.prevLogTerm) {
				event := AppendEntriesRespEv{
					id:      sm.id,
					term:    sm.currentTerm,
					success: false}
				// go PutEventToChannel(Send{cmd.leaderId,event})
				node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}
				break
			}
			// conflict with new entry

			val, _ = sm.log.Get(cmd.prevLogIndex + 1)
			val1 = val.(Entry)

			if (sm.logLastIndex >= (cmd.prevLogIndex + 1)) && (val1.term != cmd.logEntries.term) {
				sm.logLastIndex = cmd.prevLogIndex // ignore all conflicting entries
			}
			// append new entries

			val, _ = sm.log.Get(cmd.prevLogIndex)
			val1 = val.(Entry)

			if (sm.logLastIndex == cmd.prevLogIndex) && (val1.term == cmd.prevLogTerm) {

				file, err := log.Open(node.logDirectory)
				fmt.Print(err)
				for i := sm.logLastIndex + 1; i <= cmd.lengthOfLogEnty; i++ {
					//			   				fmt.Print("ONE",cmd.logEntries[i])
					//			   				sm.log[i] = cmd.logEntries[i] Append data

					err = file.Append(Entry{term: sm.currentTerm, command: cmd.logEntries.command})
				}

				sm.logLastIndex = sm.logLastIndex + (cmd.lengthOfLogEnty - sm.logLastIndex)
				event := AppendEntriesRespEv{
					id:      sm.id,
					term:    sm.currentTerm,
					success: true}
				//			   			go PutEventToChannel(Send{cmd.leaderId,event})
				node.server.Outbox() <- &cluster.Envelope{Pid: cmd.leaderId, MsgId: GlobalMsgId, Msg: event}

			}

			if cmd.leaderCommit > sm.commitIndex {
				if cmd.leaderCommit < sm.logLastIndex {
					sm.commitIndex = cmd.leaderCommit
				} else {
					sm.commitIndex = sm.logLastIndex
				}
			}
		}

	case AppendEntriesRespEv:

		fmt.Print("\nAppendEntriesRespEv Message")

		cmd := ev.(AppendEntriesRespEv)
		if sm.state == "LEADER" {
			if cmd.term > sm.currentTerm { // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term
			} else if cmd.term < sm.currentTerm {
				// out of date packet or ignore
			} else {
				if cmd.success { // 
					// increment count of majority
					sm.replicateCount = sm.replicateCount + 1
					if sm.replicateCount == 3 { // replicated in majority of servers

						if sm.commitIndex < sm.logLastIndex { // got majority vote from peers to commit the data

							//event := Commit{index: sm.commitIndex, err: ""}
							event := CommitInfo{Index :sm.logLastIndex, err: nil}

							var i int64
							for i = sm.commitIndex+1; i <= sm.logLastIndex; i++ {
								val, _ := sm.log.Get(i)
								val1 := val.(Entry)
								event.Data[i] = val1
							}

							sm.commitIndex = sm.logLastIndex // update commit index
							sm.nextIndex[cmd.id] = sm.logLastIndex + 1
							sm.matchedIndex[cmd.id] = sm.logLastIndex

							node.commitCh <- event
//							node.server.Outbox() <- &cluster.Envelope{Pid: sm.leaderId, MsgId: GlobalMsgId, Msg: event}
							//go PutEventToChannel(event)
						}
					}
				} else { // log nconsistency, retry AppenedEntryRPC request

					sm.nextIndex[cmd.id] = sm.nextIndex[cmd.id] - 1

					val, _ := sm.log.Get(sm.logLastIndex - 1)
					val1 := val.(Entry)

					event := AppendEntriesReqEv{
						term:            sm.currentTerm,
						leaderId:        sm.id,
						prevLogIndex:    sm.logLastIndex - 1,
						prevLogTerm:     val1.term,
						lengthOfLogEnty: 1,
						leaderCommit:    sm.commitIndex}

					// go PutEventToChannel(Send{cmd.id,event})
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.id, MsgId: GlobalMsgId, Msg: event}

				}
			}
		} else if sm.state == "CANDIDATE" {

			if cmd.term > sm.currentTerm { // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term
			}

		} else if sm.state == "FOLLOWER" {

			if cmd.term > sm.currentTerm { // new leader/ leader with higher term exist
				sm.currentTerm = cmd.term
			}
		}

	case VoteReqEv:

		fmt.Print("\nVoteReqEv Message")

		cmd := ev.(VoteReqEv)
		//fmt.Print("\n",cmd.term, sm.currentTerm)

		if cmd.term < sm.currentTerm {

			fmt.Print("\n", node.nodeId, " : Mine is Higher Term")
			GlobalMsgId = GlobalMsgId + 1
			//				go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: false}})
			node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: sm.currentTerm, voteGranted: false}}

		} else {

			fmt.Print("\n", node.nodeId, " : Confirtable Terms")

			if cmd.term > sm.currentTerm {
				fmt.Print("\n", node.nodeId, " : inside 1")
				sm.currentTerm = cmd.term
				sm.state = "FOLLOWER"
				sm.votedFor[cmd.term] = 0 // first time seeing, not voted
			}

			GlobalMsgId = GlobalMsgId + 1
			if sm.votedFor[cmd.term] == 0 || sm.votedFor[cmd.term] == sm.id {
				if (sm.logLastTerm > cmd.lastLogTerm) ||
					(sm.logLastTerm == cmd.lastLogTerm && sm.logLastIndex > cmd.lastLogIndex) {
					// my log entries are latest compared to candidate log entry
					fmt.Print("\n", node.nodeId, " : inside 2")
					//					go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: false}})
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: sm.currentTerm, voteGranted: false}}
				} else {
					// candidate log entries are latest
					fmt.Print("\n", node.nodeId, " : inside 3, To :", cmd.candidateId)
					sm.votedFor[cmd.term] = cmd.candidateId //voted
					//					go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: true}})
					node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: sm.currentTerm, voteGranted: true}}
				}
			} else { // already voted for this term
				//					go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: false}})
				fmt.Print("\n", node.nodeId, " : inside 4 : already Voted")
				node.server.Outbox() <- &cluster.Envelope{Pid: cmd.candidateId, MsgId: GlobalMsgId, Msg: VoteRespEv{term: sm.currentTerm, voteGranted: false}}
			}
		}

	case VoteRespEv:

		fmt.Print("VoteRespEv Mesage")
		cmd := ev.(VoteRespEv)

		if sm.state == "LEADER" {

			if cmd.term > sm.currentTerm { // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term
			}
		} else if sm.state == "CANDIDATE" {

			if cmd.term > sm.currentTerm { // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term

			} else if cmd.term < sm.currentTerm { // This case won't happen
				
				
				//
				//

			} else {
				if cmd.voteGranted {
					sm.voteCount[sm.currentTerm] = sm.voteCount[sm.currentTerm] + 1 // increment votecount

					if sm.voteCount[sm.currentTerm] == 3 {
						sm.state = "LEADER"
						// send heartbeat msg
						// for all other servers

						val, _ := sm.log.Get(sm.logLastIndex) // check for (sm.logLastIndex - 1)
 
						val1 := val.(Entry)


							event := AppendEntriesReqEv{
								term:            sm.currentTerm,
								leaderId:        sm.id,
								prevLogIndex:    sm.logLastIndex,
								prevLogTerm:     val1.term,
								lengthOfLogEnty: 0,
								leaderCommit:    sm.commitIndex}
							node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}
						

					}
				}
			}
		} else if sm.state == "FOLLOWER" {
			if cmd.term > sm.currentTerm { // update the term
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term
			}
		}

	case Timeout:

		fmt.Print("Timeout Mesage")

		if sm.state == "LEADER" {
			//Send heartbeat to all peers

			val, _ := sm.log.Get(sm.logLastIndex) // check for (sm.logLastIndex - 1)
			val1 := val.(Entry)
			event := AppendEntriesReqEv{
				term:            sm.currentTerm,
				leaderId:        sm.id,
				prevLogIndex:    sm.logLastIndex,
				prevLogTerm:     val1.term,
				lengthOfLogEnty: 0,
				leaderCommit:    sm.commitIndex}
			GlobalMsgId++
			// send to all peers
			node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}

		} else if sm.state == "CANDIDATE" {
			// Timeout means election timeout';;''

			fmt.Print("I am CANDIDATE :", sm.id)
			// election timeut
			sm.currentTerm = sm.currentTerm + 1
			//sm.state = "CANDIADTE"
			sm.votedFor[sm.currentTerm] = sm.id
			sm.voteCount[sm.currentTerm] = 0

			val, _ := sm.log.Get(sm.logLastIndex)

			val1 := val.(Entry)
			event := VoteReqEv{
				term:         sm.currentTerm,
				candidateId:  sm.id,
				lastLogIndex: sm.logLastIndex,
				lastLogTerm:  val1.term}
			GlobalMsgId = GlobalMsgId + 1
			node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}

			/*			for i:= 1; i<= len(sm.peers);i++ {
						go PutEventToChannel(Send{sm.peers[i-1],event})
					}*/

		} else if sm.state == "FOLLOWER" {

			//did n't receive heartbeat, election starts!!

			fmt.Print("I am FOLLOWER :", sm.id)
			// election timeout
			sm.currentTerm = sm.currentTerm + 1
			sm.state = "CANDIDATE"
			sm.votedFor[sm.currentTerm] = sm.id
			sm.voteCount[sm.currentTerm] = 0
			val, _ := sm.log.Get(sm.logLastIndex)
			val1 := val.(Entry)
			event := VoteReqEv{
				term:         sm.currentTerm,
				candidateId:  sm.id,
				lastLogIndex: sm.logLastIndex,
				lastLogTerm:  val1.term}

			GlobalMsgId = GlobalMsgId + 1
			node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: event}
			/*for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}*/
		}
	}
}
