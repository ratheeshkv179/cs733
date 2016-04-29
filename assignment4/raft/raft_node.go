package raft

/**
Ratheesh k v
CS 733
IIT Bombay
*/

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"github.com/cs733-iitb/log"
	"github.com/ratheeshkv179/cs733/assignment5/fs"
	"os"
	"time"
)

// Global message ID
var GlobalMsgId int64 = 0

// Mockcluster instance
var Mcluster *mock.MockCluster

// fart node instances
var Node1, Node2, Node3, Node4, Node5 *RaftNode

// client handler channels
var ClientHandlerChannel [20]chan CommitInfo

// response of Append from a node comes out as CommitInfo to commit channel
type CommitInfo struct {
	Data  Entry //Data  []byte
	Index int64 // index valid if err == nil
	Err   error // Any error on App)
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

	nodeId           int              // server ID
	eventCh          chan interface{} // event channel to receive append event
	timeoutCh        chan interface{} // timeout channel to receive timeout event
	commitCh         chan CommitInfo  // response to Append Event
	Sm               StateMachine     // State machine instance
	logDirectory     string           // log file location
	peers            []int            // Peer id list
	electionTimeout  time.Duration    // election timeout duration
	heartbeatTimeout time.Duration    // heartbeat timeout duration
	server           *mock.MockServer // corresponding server for sending and receiving message from network

	nodeTimer *time.Timer // node's timer handler
}

// Get handle to timer {old timer, not using}
func getTimer(d time.Duration) *time.Ticker {
	var ticker *time.Ticker
	ticker = time.NewTicker(time.Millisecond * d)
	return ticker
}

// start my timer {old timer, not using}
func startTimer(node *RaftNode, ticker *time.Ticker) {
	fmt.Print("")

	go func() {
		for _ = range ticker.C {
			event := Timeout{}
			node.Sm.ProcessEvent(node, event) // Process timer event
		}
	}()
}

// Stop timer {old timer, not using}
func stopHeartBeatTimer(node *RaftNode) {
	//	node.heartbeatTimer.Stop()
}

// Stop timer {old timer, not using}
func stopElectionTimer(node *RaftNode) {
	//	node.electionTimer.Stop()
}

//  Get handle to my timer {currently using}
func getNewTimer(d time.Duration) *time.Timer {
	timer := time.NewTimer(d * time.Millisecond)
	return timer
}

//  Start my timer currently using}
func startNewTimer(node *RaftNode, timer *time.Timer) {

	go func() {
		////fmt.Println("\n1Before Timer blows! ", time.Now())
		<-timer.C
		node.timeoutCh <- Timeout{}

		if node.Sm.State == "LEADER" {
		}

	}()
}

// initialize the cluster with given configuration
func InitiateCluster(conf cluster.Config) {
	Mcluster, _ = mock.NewCluster(conf)
}

// create a new raft node with given configuration
func New(config Config) (node *RaftNode) {

	// register object type, so as to send via Inbox
	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesRespEv{})
	gob.Register(VoteReqEv{})
	gob.Register(VoteRespEv{})

	var rnode *RaftNode = new(RaftNode)              // objectinstance
	rnode.nodeId = config.Id                         // node id
	rnode.eventCh = make(chan interface{}, 10000)    //event channel, get append request from here
	rnode.timeoutCh = make(chan interface{}, 100000) // timeout channel, get timeout events
	rnode.commitCh = make(chan CommitInfo, 10000)    // commit channel, reply to client

	// State machine instance
	rnode.Sm = StateMachine{id: rnode.nodeId,
		leaderId:    -1,
		State:       "FOLLOWER",
		commitIndex: -1,
		lastApplied: -1}

	rnode.Sm.currentTerm = 0                                 // strting term with 0
	rnode.Sm.votedFor = make(map[int]int)                    // voted to which node for which term
	rnode.Sm.nextIndex = make([]int64, 6)                    // next index of others nodes
	rnode.Sm.matchedIndex = make([]int64, 6)                 // matched index to others
	rnode.Sm.voteCount = make([]int, 2)                      // success or fail vote count
	rnode.logDirectory = config.LogDir                       //  my log location
	rnode.peers = Mcluster.Servers[rnode.nodeId].Peers()     // peer id list
	rnode.electionTimeout = config.ElectionTimeout           // timeout
	rnode.heartbeatTimeout = config.HeartbeatTimeout         // timeout
	rnode.nodeTimer = getNewTimer(rnode.electionTimeout)     // handle to timer
	rnode.server = Mcluster.Servers[rnode.nodeId]            // server instance
	rnode.Sm.log, _ = log.Open(rnode.logDirectory)           // node's log
	rnode.Sm.log.RegisterSampleEntry(Entry{})                // register log entry type
	err := rnode.Sm.log.Append(Entry{Term: 0, Command: nil}) // Dummy entry added to log's 0th index
	_ = err
	rnode.Sm.commitIndex = 0 // commit index
	for _, j := range rnode.peers {
		rnode.Sm.nextIndex[j] = rnode.Sm.log.GetLastIndex() + 1 // initializa nextIndex to other node
	}
	return rnode // return initialized Raftnode
}

// start the raft node
func (node *RaftNode) Start() {
	// start the timer
	startNewTimer(node, node.nodeTimer)

	go func() { // go routine for listening on inbox
		for {
			InboxEvent := <-node.server.Inbox()        //  get inbox event
			node.Sm.ProcessEvent(node, InboxEvent.Msg) // process the inbox event
		}
	}()
	// Start processing append/ timer even
	node.processEvents()

}

type Node interface {

	// Client's message to Raft node
	Append(fs.Msg)

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

func (rn *RaftNode) Append(msg *fs.Msg /*data string*/) {

	rn.eventCh <- Append{data: msg}
}

// Last known committed index in the log.
func (rn *RaftNode) CommittedIndex() int64 {
	return rn.Sm.commitIndex

}

// get commichannel events
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
	if rn.Sm.leaderId > 0 {
		return rn.Sm.leaderId
	} else {
		return -1
	}

}

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
func (rn *RaftNode) Shutdown() {
	// close the node
	rn.server.Close()
	rn.Sm.State = "CLOSED"
	rn.nodeTimer.Stop()
}

// Raft node restart {not working}
func (rn *RaftNode) Restart() {
	rn.server.Heal()
	rn.Sm.State = "FOLLOWER"
	rn.nodeTimer.Reset(rn.electionTimeout * time.Millisecond)
	startNewTimer(rn, rn.nodeTimer)
	for _, j := range rn.peers {
		rn.Sm.nextIndex[j] = rn.Sm.log.GetLastIndex() + 1
		rn.Sm.matchedIndex[j] = 0
		////fmt.Print("\n I :", i, ", J :", j)
	}
}

func (node *RaftNode) processEvents() { // start processing the events (waiting in channel for receiving events)
	//go routine for each node to process event
	go node.processAppendEvent()
	//go routine for each node to process timer event
	go node.processTimer()

}

//go routine for each node to process event
func (node *RaftNode) processAppendEvent() {
	for {
		event := <-node.eventCh
		if node.Sm.leaderId == -1 { // Don't know the leader
			obj := CommitInfo{Err: errors.New("ERR : Don't Know leader")}
			node.commitCh <- obj
		} else if node.Sm.leaderId == node.nodeId { // I am the leader
			// Process myself
			node.Sm.ProcessEvent(node, event)

		} else { // I am not leader, send back error reply
			obj := CommitInfo{Err: errors.New("ERR_REDIRECT")}
			node.commitCh <- obj
		}
	}
}

// To process the timer event
func (node *RaftNode) processTimer() {
	for {
		event := <-node.timeoutCh
		go node.Sm.ProcessEvent(node, event)
	}
}

// FIlesystem backend listen to commit channel and give reply  to ech node's client handler
func backEndFS() {
	for {
		select {
		case e1 := <-Node1.commitCh:
			ClientHandlerChannel[e1.Data.Command.ClientId] <- e1
		case e2 := <-Node2.commitCh:
			ClientHandlerChannel[e2.Data.Command.ClientId] <- e2
		case e3 := <-Node3.commitCh:
			ClientHandlerChannel[e3.Data.Command.ClientId] <- e3
		case e4 := <-Node4.commitCh:
			ClientHandlerChannel[e4.Data.Command.ClientId] <- e4
		case e5 := <-Node5.commitCh:
			ClientHandlerChannel[e5.Data.Command.ClientId] <- e5
		}
	}
}

func InitSystem() {

	fmt.Print("")

	// Remove existing log files
	os.RemoveAll("one")
	os.RemoveAll("two")
	os.RemoveAll("three")
	os.RemoveAll("four")
	os.RemoveAll("five")

	// Each node configuration
	config1 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               1,
		LogDir:           "one",
		ElectionTimeout:  900,
		HeartbeatTimeout: 300}

	config2 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               2,
		LogDir:           "two",
		ElectionTimeout:  920,
		HeartbeatTimeout: 300}

	config3 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               3,
		LogDir:           "three",
		ElectionTimeout:  1030,
		HeartbeatTimeout: 300}

	config4 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               4,
		LogDir:           "four",
		ElectionTimeout:  1300,
		HeartbeatTimeout: 300}

	config5 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               5,
		LogDir:           "five",
		ElectionTimeout:  1050,
		HeartbeatTimeout: 300}

	// Mock cluster configuration
	clconfig := cluster.Config{Peers: []cluster.PeerConfig{
		{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5},
	},
		InboxSize:  1000,
		OutboxSize: 1000}

	// Initialize cluster
	InitiateCluster(clconfig)

	// create each node
	Node1 = New(config1)
	Node2 = New(config2)
	Node3 = New(config3)
	Node4 = New(config4)
	Node5 = New(config5)

	// start each node
	Node1.Start()
	Node2.Start()
	Node3.Start()
	Node4.Start()
	Node5.Start()

	// start the filesystem backend
	go backEndFS()

}
