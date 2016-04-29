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
	Index int64   // index valid if err == nil
	err   error   // Any error on Append event
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

	nodeId           int // server ID
	eventCh          chan interface{}
	timeoutCh        chan interface{}
	commitCh         chan CommitInfo // response to Append Event
	sm               StateMachine    //
	logDirectory     string          // log file location
	peers            []int
	electionTimeout  time.Duration //
	heartbeatTimeout time.Duration
	server           *mock.MockServer // corresponding server for sending and receiving message from network
	//*	electionTimer    *time.Timer      //*time.Ticker
	//*	heartbeatTimer   *time.Timer      //*time.Ticker

	nodeTimer *time.Timer
}

func getTimer(d time.Duration) *time.Ticker {
	var ticker *time.Ticker
	ticker = time.NewTicker(time.Millisecond * d)
	return ticker
}

func startTimer(node *RaftNode, ticker *time.Ticker) {
	fmt.Print("")

	if node.sm.state == "LEADER" {
		////fmt.Print("\n[", node.nodeId, "-", node.sm.state, "] HeartBeat Timer Started", time.Now())
	} else if node.sm.state == "FOLLOWER" || node.sm.state == "CANDIDATE" {
		////fmt.Print("\n[", node.nodeId, "-", node.sm.state, "] Election Timer Started", time.Now())
	}

	go func() {
		for _ = range ticker.C {
			//			//fmt.Print("\nTimeout at [: ", node.nodeId, " - ", node.sm.state, "] ", time.Now())
			//node.timeoutCh <-
			event := Timeout{}
			node.sm.ProcessEvent(node, event)

			if node.sm.state == "LEADER" {
				////fmt.Print("\n[", node.nodeId, "-", node.sm.state, "] HeartBeat Timer Started", time.Now())
				//	//fmt.Print("\nHeartbeat Timeout LEADER ", node.nodeId, "  ", node.sm.state, " ", time.Now())
			} else if node.sm.state == "FOLLOWER" {
				////fmt.Print("\n[", node.nodeId, "-", node.sm.state, "] Election Timer Started", time.Now())
				//		//fmt.Print("\nElection Timeout FOLLOWER ", node.nodeId, "  ", node.sm.state, " ", time.Now())
			} else if node.sm.state == "CANDIDATE" {
				//			//fmt.Print("\nElection Timeout CANDIDATE ", node.nodeId, "  ", node.sm.state, " ", time.Now())
			}

			//obj := Entry{term:1, command: "hai ratheesh"}
			//eventChannel <- obj
			// //fmt.Println("Tick at [",id,"] : ", t)
		}
	}()
}

func stopHeartBeatTimer(node *RaftNode) {
	//	node.heartbeatTimer.Stop()
}

func stopElectionTimer(node *RaftNode) {
	//	node.electionTimer.Stop()
}

func getNewTimer(d time.Duration) *time.Timer {
	timer := time.NewTimer(d * time.Millisecond)
	return timer
}

func startNewTimer(node *RaftNode, timer *time.Timer) {

	go func() {
		////fmt.Println("\n1Before Timer blows! ", time.Now())
		<-timer.C
		node.timeoutCh <- Timeout{}

		if node.sm.state == "LEADER" {
			//	//fmt.Print("\n\n#### TIMER  : Id:", node.nodeId, " - ", time.Now(), "####\n\n")
		}

		////fmt.Println("\n1Timer blows! ", time.Now())
	}()
}

/*
func (node *RaftNode) resetTimer() {
	if node.sm.state == "LEADER" {
		//fmt.Print("\nHeartbeatTimer started")
		select {
		//case <-time.After(time.Second * time.Duration((rand.Intn(20-5) + 5))):
		case <-time.After(time.Microsecond * node.heartbeatTimeout):
			node.timeoutCh <- Timeout{}
			//fmt.Print("\nHeartBeat Timeout")
		}
	} else if node.sm.state == "CANDIDATE" || node.sm.state == "FOLLOWER" {
		//fmt.Print("\nElection Timer started")
		select {
		//case <-time.After(time.Second * time.Duration((rand.Intn(20-5) + 5))):
		case <-time.After(time.Microsecond * node.electionTimeout):
			node.timeoutCh <- Timeout{}
			//fmt.Print("\nElection Timeout")
		}
	}
}
*/

func InitiateCluster(conf cluster.Config) {
	Mcluster, _ = mock.NewCluster(conf)
}

func New(config Config) (node *RaftNode) {

	gob.Register(AppendEntriesReqEv{})
	gob.Register(AppendEntriesRespEv{})
	gob.Register(VoteReqEv{})
	gob.Register(VoteRespEv{})

	var rnode *RaftNode = new(RaftNode)

	rnode.nodeId = config.Id
	rnode.eventCh = make(chan interface{}, 10000)
	rnode.timeoutCh = make(chan interface{}, 100000)
	rnode.commitCh = make(chan CommitInfo, 10000)

	rnode.sm = StateMachine{id: rnode.nodeId,
		leaderId:     -1,
		state:        "FOLLOWER",
		logLastIndex: -1,
		logLastTerm:  0,
		commitIndex:  -1,
		lastApplied:  -1}

	rnode.sm.currentTerm = 0
	rnode.sm.votedFor = make(map[int]int)
	rnode.sm.eventChannel = make(chan interface{}, 1000)
	rnode.sm.actionChannel = make(chan interface{}, 1000)
	rnode.sm.clientChannel = make(chan interface{}, 1000)

	rnode.sm.nextIndex = make([]int64, 6)
	rnode.sm.matchedIndex = make([]int64, 6)
	//rnode.sm.voteCount = make([]int, 1000000)

	rnode.sm.voteCount = make([]int, 2)

	rnode.logDirectory = config.LogDir
	rnode.peers = Mcluster.Servers[rnode.nodeId].Peers()

	/*//fmt.Print("\nMy ID : ", rnode.nodeId, "\nPeers ID : ")
	for i, j := range rnode.peers {
		//fmt.Print("\n I :", i, ", J :", j)
	}*/

	rnode.electionTimeout = config.ElectionTimeout
	rnode.heartbeatTimeout = config.HeartbeatTimeout

	//*	rnode.electionTimer = getTimer(rnode.electionTimeout)
	//*	rnode.heartbeatTimer = getTimer(rnode.heartbeatTimeout)

	rnode.nodeTimer = getNewTimer(rnode.electionTimeout)

	rnode.server = Mcluster.Servers[rnode.nodeId]

	rnode.sm.log, _ = log.Open(rnode.logDirectory)
	////fmt.Print("\nERR : ", err)
	//file.RegisterSampleEntry(Entry{})
	//rnode.sm.log = file
	rnode.sm.log.RegisterSampleEntry(Entry{})
	////fmt.Print("\nindx + ", rnode.sm.log.GetLastIndex())
	err := rnode.sm.log.Append(Entry{Term: 0, Command: "dummy"}) // Dummy entry added to log's 0th index
	_ = err
	////fmt.Print("\nErr :", err, " + ", rnode.sm.log.GetLastIndex())
	rnode.sm.logLastIndex = rnode.sm.log.GetLastIndex()
	rnode.sm.commitIndex = 0

	for _, j := range rnode.peers {
		rnode.sm.nextIndex[j] = rnode.sm.log.GetLastIndex() + 1
		////fmt.Print("\n I :", i, ", J :", j)
	}

	//node = rnode
	return rnode
}

func (node *RaftNode) Start() {

	/*	go func() {
			//    //fmt.Printf("\nWaiting for Inbox Message")
			for {
				event := <-node.eventCh
				event1 := event.(Append)
				//fmt.Print("\nEVENT : ", event)
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

	//*	startTimer(node, node.electionTimer)

	startNewTimer(node, node.nodeTimer)

	//	//fmt.Print("\n\n$$$$$$ : ", node.nodeId, " : $$$$$$$")

	go func() { // go routine for listening on inbox
		for {
			InboxEvent := <-node.server.Inbox()
			//	//fmt.Print("\nEVENT in Inbox [", node.nodeId, "]")
			//	//fmt.Println("---->>", node.nodeId)

			node.sm.ProcessEvent(node, InboxEvent.Msg)
			//	//fmt.Println("<<----", node.nodeId)
			//fmt.Print("")

		}
	}()

	node.processEvents()

	//go node.resetTimer()
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
	rn.sm.state = "CLOSED"
	//	rn.heartbeatTimer.Stop()
	//	rn.electionTimer.Stop()
	rn.nodeTimer.Stop()
}

func (rn *RaftNode) Restart() {

	rn.server.Heal()
	rn.sm.state = "FOLLOWER"

	rn.nodeTimer.Reset(rn.electionTimeout * time.Millisecond)
	startNewTimer(rn, rn.nodeTimer)

	for _, j := range rn.peers {
		rn.sm.nextIndex[j] = rn.sm.log.GetLastIndex() + 1
		rn.sm.matchedIndex[j] = 0
		////fmt.Print("\n I :", i, ", J :", j)
	}

}

func (node *RaftNode) processEvents() { // start processing the events (waiting in channel for receiving events)

	go node.processAppendEvent()
	go node.processTimer()

	//	//fmt.Print("\n\nNODE ID : ", node.nodeId)
	/*for {
		select {
		case event := <-node.eventCh:
			event1 := event.(Append)
			if node.sm.leaderId == -1 { // Don't know the leader
				// Give error message to CommitChannel
				//fmt.Print("\nDon't Know leader")
				obj := CommitInfo{err: errors.New("Don't know the leader")}
				node.commitCh <- obj
			} else if node.sm.leaderId == node.nodeId { // I am the leader
				// Process myself
				node.sm.ProcessEvent(node, event)
			} else { // forward to the leader
				//fmt.Print("\n[", node.nodeId, "] Forward to Leader : ", node.sm.leaderId)
				obj := Append{data: event1.data}
				GlobalMsgId++
				node.server.Outbox() <- &cluster.Envelope{Pid: node.sm.leaderId, MsgId: GlobalMsgId, Msg: obj}
			}
		case event := <-node.timeoutCh:
			//fmt.Print("\n$$$$$TIMEOUT ", node.sm.state, node.nodeId, " ", time.Now())
			go node.sm.ProcessEvent(node, event)
			//	default:
			//		//fmt.Print("\nNot Matching?")
		}
	}*/

}

func (node *RaftNode) processAppendEvent() {
	for {
		event := <-node.eventCh
		event1 := event.(Append)
		if node.sm.leaderId == -1 { // Don't know the leader
			// Give error message to CommitChannel
			////fmt.Print("\nDon't Know leader")
			obj := CommitInfo{err: errors.New("Don't know the leader")}
			node.commitCh <- obj
		} else if node.sm.leaderId == node.nodeId { // I am the leader
			// Process myself
			node.sm.ProcessEvent(node, event)
			//	//fmt.Println("<<----", node.nodeId)

		} else { // forward to the leader
			////fmt.Print("\n[", node.nodeId, "] Forward to Leader : ", node.sm.leaderId)
			obj := Append{data: event1.data}
			GlobalMsgId++
			node.server.Outbox() <- &cluster.Envelope{Pid: node.sm.leaderId, MsgId: GlobalMsgId, Msg: obj}
		}
	}
}

func (node *RaftNode) processTimer() {
	for {
		event := <-node.timeoutCh
		//		//fmt.Print("\n$$$$$TIMEOUT ", node.sm.state, node.nodeId, " ", time.Now())
		go node.sm.ProcessEvent(node, event)
	}
}
