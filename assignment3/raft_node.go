package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"github.com/cs733-iitb/log"
	"time"
)

// response of Append from a node comes out as CommitInfo
type CommitInfo struct {
	//Data  []byte //
	Data  []Entry //
	Index int64  // index valid if err == nil
	err   error  // Any error on Append event
}

// This is an example structure for Config .. change it to your convenience.
type Config struct {
	cluster          []NetConfig   // Information about all servers, including this.
	Id               int           // this node's id. One of the cluster's entries should match.
	LogDir           string        // Log file directory for this node
	ElectionTimeout  time.Duration // election timeout
	HeartbeatTimeout time.Duration // heartbeat timeout
}

// Network configuration
type NetConfig struct {
	Id   int
	Host string
	Port int
}

// structure of raft node
type RaftNode struct { // implements Node interface

	nodeId           int //
	eventCh          chan interface{}
	timeoutCh        chan interface{}
	commitCh         chan CommitInfo // response to Append Event
	sm               StateMachine    //
	logDirectory     string          // log file location
	electionTimeout  time.Duration   //
	heartbeatTimeout time.Duration
	server           *mock.MockServer // corresponding server for sending and receiving message from network
	electionTimer    *time.Ticker
	heartbeatTimer   *time.Ticker
}

func getTimer(d time.Duration) *time.Ticker {
	var ticker *time.Ticker
	ticker = time.NewTicker(time.Millisecond * d)
	return ticker
}

func startTimer(node *RaftNode, ticker *time.Ticker) {
	go func() {
		for _ = range ticker.C {

			fmt.Print("****Timeout at : ", node.nodeId, "****")
			node.timeoutCh <- Timeout{}
			//obj := Entry{term:1, command: "hai ratheesh"}
			//eventChannel <- obj
			// fmt.Println("Tick at [",id,"] : ", t)

		}
	}()
}

func stopHeartBeatTimer(node *RaftNode) {
	node.heartbeatTimer.Stop()
}

func stopElectionTimer(node *RaftNode) {
	node.electionTimer.Stop()
}

func InitiateCluster(conf cluster.Config) {
	Mcluster, _ = mock.NewCluster(conf)
}

func New(config Config) (node *RaftNode) {

	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesRespEv{})
	gob.Register(VoteReqEv{})
	gob.Register(VoteRespEv{})
	rnode := new(RaftNode)

	rnode.nodeId = config.Id
	rnode.eventCh = make(chan interface{}, 10)
	rnode.timeoutCh = make(chan interface{}, 10)
	rnode.commitCh = make(chan CommitInfo, 10)

	rnode.sm = StateMachine{id: rnode.nodeId, leaderId: -1, state: "FOLLOWER", logLastIndex: -1, logLastTerm: 0, commitIndex: -1, lastApplied: -1}
	rnode.sm.votedFor = make(map[int]int)
	rnode.sm.eventChannel = make(chan interface{}, 10)
	rnode.sm.actionChannel = make(chan interface{}, 10)
	rnode.sm.clientChannel = make(chan interface{}, 10)

	rnode.logDirectory = config.LogDir
	rnode.electionTimeout = config.ElectionTimeout
	rnode.heartbeatTimeout = config.HeartbeatTimeout

	rnode.electionTimer = getTimer(rnode.electionTimeout)
	rnode.heartbeatTimer = getTimer(rnode.heartbeatTimeout)

	rnode.server = Mcluster.Servers[rnode.nodeId]

	file, err := log.Open(rnode.logDirectory)
	fmt.Print("\nERR? : ", err)
	file.RegisterSampleEntry(Entry{})
	rnode.sm.log = file
	_ = rnode.sm.log.Append(Entry{term: 0, command: "dummy"}) // Dummy entry added to log's 0th index
	rnode.sm.logLastIndex = rnode.sm.log.GetLastIndex()
	rnode.sm.commitIndex = 0
	node = rnode
	return node
}

func (node *RaftNode) Start() {

	/*	go func() {
			//    fmt.Printf("\nWaiting for Inbox Message")
			for {
				event := <-node.eventCh
				event1 := event.(Append)
				fmt.Print("\nEVENT : ", event)
				if node.sm.leaderId == -1 { // Don't know the leader
					// Give error message to CommitChannel
					obj := CommitInfo{err: nil}
					node.commitCh <- obj
				} else if node.sm.leaderId == node.nodeId { // I am the leader
					// Process myself
					node.sm.ProcessEvent(node, event)
				} else { // forward to the leader
					GlobalMsgId++
					node.server.Outbox() <- &cluster.Envelope{Pid: node.sm.leaderId, MsgId: GlobalMsgId, Msg: event1.data}
				}
			}
		}()
	*/

	fmt.Print("\n\n$$$$$$ : ", node.nodeId, " : $$$$$$$")

	go func() { // go routine for listening on inbox
		for {
			InboxEvent := <-node.server.Inbox()
			fmt.Print("\nEVENT in Inbox")
			node.sm.ProcessEvent(node, InboxEvent.Msg)
		}
	}()

	if node.sm.state == "LEADER" {
		startTimer(node, node.heartbeatTimer)
		fmt.Print("HeartbeatTimer started")
	} else if node.sm.state == "FOLLOWER" {
		startTimer(node, node.electionTimer)
		fmt.Print("ElectionTimer started")
	}

	//node.processEvents()

	// /time.Sleep(time.Second * 4)

}

type Node interface {

	// Client's message to Raft node
	Append(string)

	// A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() <-chan CommitInfo

	// Last known committed index in the log.
	CommittedIndex() int
	//This could be -1 until the system stabilizes.

	// Returns the data at a log index, or an error.
	Get(index int) (error, []byte)

	// Node's id
	Id()

	// Id of leader. -1 if unknown
	LeaderId() int

	// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
}

func (rn *RaftNode) Append(data string) {
	rn.eventCh <- Append{data: data}
}

// Returns the data at a log index, or an error.
func (rn *RaftNode) Get(index int64) (err error, data []byte) {

	if rn.sm.logLastIndex >= index {
		// return from log
	} else {
		return err, nil
	}
	return err, nil
}

// Last known committed index in the log.
func (rn *RaftNode) CommittedIndex() int64 {
	return rn.sm.commitIndex

}

func (rn *RaftNode) CommitChannel() CommitInfo {
	commitEvent := <-rn.commitCh
	return commitEvent
}

// Return Node's Id
func (rn *RaftNode) Id() int {
	return rn.nodeId
}

// Return  Id of leader. -1 if unknown
func (rn *RaftNode) LeaderId() int {
	if rn.sm.leaderId > 0 {
		return rn.sm.leaderId
	} else {
		return -1
	}

	// from state machine u will get
}

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
func (rn *RaftNode) Shutdown() {
	// close the node
	rn.server.Close()
}

func (node *RaftNode) processEvents() { // start processing the events (waiting in channel for receiving events)

	fmt.Print("\n\nNODE ID : ", node.nodeId)
	for {
		select {
		case event := <-node.eventCh:
			event1 := event.(Append)
			fmt.Print("\nEVENT : ", event)
			if node.sm.leaderId == -1 { // Don't know the leader
				// Give error message to CommitChannel
				fmt.Print("\nDon't Know leader")
				obj := CommitInfo{err: errors.New("Don't know the leader")}
				node.commitCh <- obj
			} else if node.sm.leaderId == node.nodeId { // I am the leader
				fmt.Print("\nI am the leader")
				// Process myself
				node.sm.ProcessEvent(node, event)
			} else { // forward to the leader
				fmt.Print("\nForward to Leader : ", node.sm.leaderId)

				obj := Append{data: event1.data}
				GlobalMsgId++
				node.server.Outbox() <- &cluster.Envelope{Pid: node.sm.leaderId, MsgId: GlobalMsgId, Msg: obj}
			}
		case event := <-node.timeoutCh:
			fmt.Print("\nTimeOut event")
			node.sm.ProcessEvent(node, event)
			//	default:
			//		fmt.Print("\nNot Matching?")
		}
	}
}
