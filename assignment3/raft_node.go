package raft

import (
        "github.com/cs733-iitb/cluster"
        "fmt"
        "time"
        "encoding/gob"
        "github.com/cs733-iitb/log"
       "github.com/cs733-iitb/cluster/mock"
)

var GlobalMsgId int64 = 0
var  Mcluster *mock.MockCluster

type  CommitInfo struct{
        Data []byte
        Index int64 // or int .. whatever you have in your code
        err error   // Err can be errred
}

// This is an example structure for Config .. change it to your convenience.
type   Config struct{
        cluster []NetConfig // Information about all servers, including this.
        Id int              // this node's id. One of the cluster's entries should match.
        LogDir string       // Log file directory for this node
        ElectionTimeout  time.Duration
        HeartbeatTimeout  time.Duration
}

type   NetConfig struct {
        Id int
        Host string
        Port int
}

type RaftNode struct { // implements Node interface

        nodeId int
        eventCh chan interface{}
        timeoutCh chan interface{}
        commitCh chan CommitInfo
        sm StateMachine
        logDirectory string
        electionTimeout time.Duration
        heartbeatTimeout time.Duration
        server *mock.MockServer
//      server cluster.Server
        //interface{}//Server
}

 
func InitiateCluster(conf cluster.Config){

//func InitiateCluster(){

    Mcluster,_ = mock.NewCluster(conf)
    //Mcluster,_ = mock.NewCluster()
    fmt.Print("\n",Mcluster,"\n")
    
    //return Mcluster
}
 

// Returns a Node object
func New(config Config) (node *RaftNode) {

        gob.Register(AppendEntriesReqEv{})
        rnode := new(RaftNode)
        rnode.nodeId = config.Id
        rnode.eventCh = make(chan interface{},10)
        rnode.timeoutCh = make(chan interface{},10)
        rnode.commitCh  = make(chan CommitInfo,10)
        rnode.sm  = StateMachine{id:rnode.nodeId,leaderId:-1,state:"FOLLOWER",logLastIndex:-1,logLastTerm:0,commitIndex:-1,lastApplied:-1}
        rnode.sm.votedFor = make(map[int]int)
        rnode.sm.eventChannel = make(chan interface{},10)   
        rnode.sm.actionChannel = make(chan interface{},10)  
        rnode.sm.clientChannel = make (chan interface{},10)  

        rnode.logDirectory = config.LogDir
        rnode.electionTimeout = config.ElectionTimeout
        rnode.heartbeatTimeout = config.HeartbeatTimeout
        // attach a server
        //server, err := cluster.New(rnode.nodeId, conf)//"cluster_test_config.json")

        fmt.Print("\n",Mcluster,"\n")
        //server,err := Mcluster.AddServer(rnode.nodeId)

        //fmt.Print("\nServer : ",server,"\nERR : ",err)
        
        rnode.server = Mcluster.Servers[rnode.nodeId]
        fmt.Print("\nServer : ",rnode.server)

        file, _ := log.Open(rnode.logDirectory);
        file.RegisterSampleEntry(Entry{});
        node = rnode
        return node
}


func (node *RaftNode) Start() {


        go func() {
                    //    fmt.Printf("\nWaiting for Inbox Message")
                for {
                      //q  fmt.Printf("\nWaiting for Inbox Message")
                        InboxEvent := <- node.server.Inbox()
                        // event := <- node.server.Inbox()
                        //fmt.Printf("\nGot Inbox Message", "server : ",node.server.Pid(),"Msg :",InboxEvent.Msg)
                      //  fmt.Printf("\n#",InboxEvent.Msg)

                         

                //      node.PutEventToChannel(node.server.Inbox())
                        node.sm.ProcessEvent(node,InboxEvent.Msg)
                }
        }()

        go func() { // Timer for ElectionTimeOut Event
                       // fmt.Printf("\nElection Timeout")
                time.Sleep(node.electionTimeout)

              //    fmt.Printf("\nElection, Server :",node.server.Pid())
                //node.eventCh = <-Timeout{} // chage to statemachine's channel
                //node.sm.ProcessEvent(Timeout{})
                node.timeoutCh <- Timeout{}
                node.GetEvents()

        }()

        go func() { // Timer for HeartBeatTimeOut Event
            //fmt.Printf("\nHeartBeat Timeout")
                time.Sleep(node.heartbeatTimeout)
              //  fmt.Printf("\nHeartBeat","Server : ",node.server.Pid())
                // Send to all other clients
                //        node.eventCh = <- AppendEntriesReqEv{term:node.sm.currentTerm,lengthOfLogEnty:0}                        
                // put to statemachine channel
                GlobalMsgId = GlobalMsgId + 1
                node.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: GlobalMsgId, Msg: AppendEntriesReqEv{term:node.sm.currentTerm,lengthOfLogEnty:0} }

        }()

}

type Node interface {

        // Client's message to Raft node
        Append([]byte)

        // A channel for client to listen on. What goes into Append must come out of here at some point.
        CommitChannel() <- chan CommitInfo

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
func  (rn *RaftNode) Get(index int) (err error,data []byte){

        if(rn.sm.logLastIndex >= index){
                // return from log
                }else {
                        return err, nil
                }
                return err, nil
}

// Last known committed index in the log.
func (rn *RaftNode)  CommittedIndex() int{
        return rn.sm.commitIndex;

}

func (rn *RaftNode)  CommitChannel()  chan CommitInfo {
        rn.commitCh  = make (chan CommitInfo)
        return rn.commitCh
}

// Return Node's Id
func (rn *RaftNode) Id() int{ 
        return rn.nodeId
}

// Return  Id of leader. -1 if unknown
func (rn *RaftNode) LeaderId() int{
        if(rn.sm.leaderId > 0 ){
                return rn.sm.leaderId
                }else {
                        return -1
                }

        // from state machine u will get
}

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
func (rn *RaftNode) Shutdown(){
        // close the node
        rn.server.Close()
}



func (rn *RaftNode) processEvents() {
        for {
                //var ev Event
                var ev interface{}
                select {
                        case ev := <- rn.eventCh:
                                    fmt.Print(ev)
                    //             {ev = msg}
                        case ev := <- rn.timeoutCh:
                                    fmt.Print(ev)
                      //           {ev = Timeout{}}
                        default: 
                                fmt.Print("\nNot Matching")
                }
        
         rn.sm.ProcessEvent(rn,ev)
        //fmt.Print(actions)
//        doActions(actions)
        }
}
