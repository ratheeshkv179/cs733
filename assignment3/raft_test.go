package raft

import (
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"testing"
	"time"
)

var GlobalMsgId int64 = 0
var Mcluster *mock.MockCluster

const CLR_R = "\x1b[31;1m"
const CLR_G = "\x1b[32;1m"
const CLR_Y = "\x1b[33;1m"
const CLR_B = "\x1b[34;1m"
const CLR_M = "\x1b[35;1m"
const CLR_END = "\x1b[0m"

func TestStateMachine(t *testing.T) {

	//t.Fatal("Got different data")
	config1 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               1,
		LogDir:           "one",
		ElectionTimeout:  10000,
		HeartbeatTimeout: 1000}

	config2 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               2,
		LogDir:           "two",
		ElectionTimeout:  8000,
		HeartbeatTimeout: 1000}

	config3 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               3,
		LogDir:           "three",
		ElectionTimeout:  7000,
		HeartbeatTimeout: 1000}

	config4 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               4,
		LogDir:           "four",
		ElectionTimeout:  9000,
		HeartbeatTimeout: 1000}

	config5 := Config{
		cluster:          []NetConfig{{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5}},
		Id:               5,
		LogDir:           "five",
		ElectionTimeout:  10000,
		HeartbeatTimeout: 1000}

	clconfig := cluster.Config{Peers: []cluster.PeerConfig{
		{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5},
	}}

	InitiateCluster(clconfig)

	//InitiateCluster()

	node1 := New(config1)
	//fmt.Print("\n Node 1 : ", node1)
	node2 := New(config2)
	//fmt.Print("\n Node 2 : ", node2)
	node3 := New(config3)
	//fmt.Print("\n Node 3 : ", node3)
	node4 := New(config4)
	//fmt.Print("\n Node 4 : ", node4)
	node5 := New(config5)
	//fmt.Print("\n Node 5 : ", node5)

	//node1.processEvents()

	/*  go func() {
	        for {
	            env := <- node3.server.Inbox()
	            fmt.Print(node3.nodeId)
	            fmt.Printf("[From: %d MsgId:%d] %s\n", env.Pid, env.MsgId, env.Msg)
	        }
	    }()
	    node1.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: 1, Msg: "hello"}
	    node2.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: 1, Msg: "hello"}
	    node3.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: 1, Msg: "hello"}
	    node4.server.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: 1, Msg: "hello"}
	*/

	node1.Start()
	node2.Start()
	node3.Start()
	node4.Start()
	node5.Start()

	go node1.processEvents()
	go node2.processEvents()
	go node3.processEvents()
	go node4.processEvents()
	go node5.processEvents()

	time.Sleep(time.Second * 10)

	fmt.Print("\n1 = ", node1.sm.state)
	// ColorPrinting(node1.nodeId)
	fmt.Print("\n2 = ", node2.sm.state)
	// ColorPrinting(node2.nodeId)
	fmt.Print("\n3 = ", node3.sm.state)
	// ColorPrinting(node3.nodeId)
	fmt.Print("\n4 = ", node4.sm.state)
	//ColorPrinting(node4.nodeId)
	fmt.Print("\n5 = ", node5.sm.state)

	node1.Append("hai RATHEESH")

	time.Sleep(time.Second * 10)

	fmt.Print("\nLast Index : 1 ", node1.sm.log.GetLastIndex())
	fmt.Print("\nLast Index : 2 ", node2.sm.log.GetLastIndex())
	fmt.Print("\nLast Index : 3 ", node3.sm.log.GetLastIndex())
	fmt.Print("\nLast Index : 4 ", node4.sm.log.GetLastIndex())
	fmt.Print("\nLast Index : 5 ", node5.sm.log.GetLastIndex())

	//ColorPrinting(node5.nodeId)
	//node1.Shutdown()
	//fmt.Print("\nCHECK : ", node1.server)
	//fmt.Print(node)
}

func ColorPrinting(nodeId int) {
	switch nodeId {
	case 1:
		fmt.Println(CLR_R + "Sample Coloring" + CLR_END)
	case 2:
		fmt.Println(CLR_G + "Sample Coloring" + CLR_END)
	case 3:
		fmt.Println(CLR_Y + "Sample Coloring" + CLR_END)
	case 4:
		fmt.Println(CLR_B + "Sample Coloring" + CLR_END)
	case 5:
		fmt.Println(CLR_M + "Sample Coloring" + CLR_END)
	default:
	}
}
