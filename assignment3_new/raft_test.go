package raft

import (
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"os"
	"strconv"
	"testing"
	"time"
)

var GlobalMsgId int64 = 0
var Mcluster *mock.MockCluster
var node1, node2, node3, node4, node5 *RaftNode

var follower int = 0
var leader int = 0
var candidate int = 0

const CLR_R = "\x1b[31;1m"
const CLR_G = "\x1b[32;1m"
const CLR_Y = "\x1b[33;1m"
const CLR_B = "\x1b[34;1m"
const CLR_M = "\x1b[35;1m"
const CLR_END = "\x1b[0m"

func TestStateMachine(t *testing.T) {

	fmt.Print("")
	os.RemoveAll("one")
	os.RemoveAll("two")
	os.RemoveAll("three")
	os.RemoveAll("four")
	os.RemoveAll("five")

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

	clconfig := cluster.Config{Peers: []cluster.PeerConfig{
		{Id: 1}, {Id: 2}, {Id: 3}, {Id: 4}, {Id: 5},
	},
		InboxSize:  1000,
		OutboxSize: 1000}

	InitiateCluster(clconfig)

	node1 = New(config1)
	node2 = New(config2)
	node3 = New(config3)
	node4 = New(config4)
	node5 = New(config5)

	node1.Start()
	node2.Start()
	node3.Start()
	node4.Start()
	node5.Start()

}

func TestLeaderElection(t *testing.T) {

	for !(node1.sm.state == "LEADER" || node2.sm.state == "LEADER" || node3.sm.state == "LEADER" ||
		node4.sm.state == "LEADER" || node5.sm.state == "LEADER") {
	}

	list := []*RaftNode{node1, node2, node3, node4, node5}

	for a := range list {
		if list[a].sm.state == "LEADER" {
			leader++
		} else if list[a].sm.state == "CANDIDATE" {
			candidate++
		} else if list[a].sm.state == "FOLLOWER" {
			follower++
		}
	}
	time.Sleep(time.Second * 1)

	if leader > 1 {
		t.Error("More than one leader")
	} else if follower < 4 && candidate > 0 {
		t.Error("After Leader election System not in stable state")
	}

	// Check for everyone know thw leader or not

	if node1.sm.state == "LEADER" {
		if !(node2.sm.leaderId == 1 && node3.sm.leaderId == 1 && node4.sm.leaderId == 1 && node5.sm.leaderId == 1) {
			t.Error("Every One not aware of leader %d", node1.nodeId)
		}
	} else if node2.sm.state == "LEADER" {
		if !(node1.sm.leaderId == 2 && node3.sm.leaderId == 2 && node4.sm.leaderId == 2 && node5.sm.leaderId == 2) {
			t.Error("Every One not aware of leader %d", node2.nodeId)
		}
	} else if node3.sm.state == "LEADER" {
		if !(node2.sm.leaderId == 3 && node1.sm.leaderId == 3 && node4.sm.leaderId == 3 && node5.sm.leaderId == 3) {
			t.Error("Every One not aware of leader %d", node3.nodeId)
		}
	} else if node4.sm.state == "LEADER" {
		if !(node2.sm.leaderId == 4 && node3.sm.leaderId == 4 && node1.sm.leaderId == 4 && node5.sm.leaderId == 4) {
			t.Error("Every One not aware of leader %d", node4.nodeId)
		}
	} else if node5.sm.state == "LEADER" {
		if !(node2.sm.leaderId == 5 && node3.sm.leaderId == 5 && node4.sm.leaderId == 5 && node1.sm.leaderId == 5) {
			t.Error("Every One not aware of leader %d", node5.nodeId)
		}
	}
}

func TestAppend(t *testing.T) {

	list := []*RaftNode{node1, node2, node3, node4, node5}

	// Testing Sendng appaend request to leader directly
	for a := range list {
		if list[a].sm.state == "LEADER" {
			for j := 1; j <= 100; j++ {
				list[a].Append("Test Command " + strconv.Itoa(j))
			}
			break
		}
	}

	//	fmt.Println("oooo")

	for {
		//fmt.Print("")
		if node1.sm.log.GetLastIndex() == 100 && node2.sm.log.GetLastIndex() == 100 && node3.sm.log.GetLastIndex() == 100 &&
			node4.sm.log.GetLastIndex() == 100 && node5.sm.log.GetLastIndex() == 100 {
			break
		}
	}
	//	fmt.Println("one")
	for a := range list {
		if list[a].sm.state == "LEADER" {
			if list[a].sm.commitIndex != 100 {
				t.Error("Entries are not replicated [Commit index expected :200 Found %d]", list[a].sm.commitIndex)
			}
			break
		}
	}

	// Testing Sendng appaend request to one of the follower directly
	for a := range list {
		if list[a].sm.state == "FOLLOWER" {
			for j := 1; j <= 100; j++ {
				list[a].Append("Test Command " + strconv.Itoa(j))
			}
			break
		}
	}

	for {
		//fmt.Print("")
		if node1.sm.log.GetLastIndex() == 200 && node2.sm.log.GetLastIndex() == 200 && node3.sm.log.GetLastIndex() == 200 &&
			node4.sm.log.GetLastIndex() == 200 && node5.sm.log.GetLastIndex() == 200 {
			break
		}
	}
	//fmt.Println("two")

	/*for a := range list {
		if list[a].sm.state == "LEADER" {
			if list[a].sm.commitIndex != 200 {
				t.Error("Entries are not replicated [Commit index expected :200 Found %d]", list[a].sm.commitIndex)
			}
			break
		}
	}*/

}

func TestPartition(t *testing.T) {

	leader = 0
	candidate = 0
	follower = 0

	for !(node1.sm.state == "LEADER" || node2.sm.state == "LEADER" || node3.sm.state == "LEADER" ||
		node4.sm.state == "LEADER" || node5.sm.state == "LEADER") {
	}

	list := []*RaftNode{node1, node2, node3, node4, node5}

	for a := range list {
		if list[a].sm.state == "LEADER" {
			leader++
		} else if list[a].sm.state == "CANDIDATE" {
			candidate++
		} else if list[a].sm.state == "FOLLOWER" {
			follower++
		}
	}

	fmt.Println("1")
	time.Sleep(time.Second * 1)

	if leader > 1 {
		t.Error("More than one leader")
	} else if follower < 4 && candidate > 0 {
		t.Error("After Leader election System not in stable state")
	}

	for j := 1; j <= 10; j++ {
		node2.Append("Testing Commands " + strconv.Itoa(j))
	}

	time.Sleep(time.Second * 3)

	// Leader to majority group

	fmt.Println("2")
	if node1.sm.state == "LEADER" || node2.sm.state == "LEADER" || node3.sm.state == "LEADER" {
		err := Mcluster.Partition([]int{1, 2, 3}, []int{4, 5})

		if err != nil {
			t.Error("Mk Cluster ERROR :", err)
		}

	} else {
		err := Mcluster.Partition([]int{1, 2}, []int{3, 4, 5})

		if err != nil {
			t.Error("Mk Cluster ERROR :", err)
		}
	}

	time.Sleep(time.Second * 3)

	for j := 11; j <= 20; j++ {
		node2.Append("hai RATHEESH " + strconv.Itoa(j))
		node4.Append("hai RATHEESH " + strconv.Itoa(j))
	}

	fmt.Println("3")
	for node1.sm.log.GetLastIndex() == 220 || node2.sm.log.GetLastIndex() == 220 || node4.sm.log.GetLastIndex() == 220 || node5.sm.log.GetLastIndex() == 220 {
	}
	fmt.Println("4")

	Mcluster.Heal()

	time.Sleep(time.Second * 2)

	for j := 21; j <= 30; j++ {
		node2.Append("hai RATHEESH " + strconv.Itoa(j))
	}

	fmt.Println("5")
	for node1.sm.log.GetLastIndex() == 230 && node2.sm.log.GetLastIndex() == 230 && node3.sm.log.GetLastIndex() == 230 && node4.sm.log.GetLastIndex() == 230 && node5.sm.log.GetLastIndex() == 230 {
	}
	fmt.Println("6")
	// Leader to minority group
	if node1.sm.state == "LEADER" || node2.sm.state == "LEADER" {

		err := Mcluster.Partition([]int{1, 2}, []int{3, 4, 5})

		if err != nil {
			t.Error("Mk Cluster ERROR :", err)
			time.Sleep(time.Second * 3)
			for j := 31; j <= 40; j++ {
				node2.Append("hai RATHEESH " + strconv.Itoa(j))
				node3.Append("hai RATHEESH " + strconv.Itoa(j))
			}
		}

	} else if node3.sm.state == "LEADER" || node4.sm.state == "LEADER" {

		err := Mcluster.Partition([]int{1, 2, 5}, []int{4, 3})

		if err != nil {
			t.Error("Mk Cluster ERROR :", err)
			time.Sleep(time.Second * 3)

			time.Sleep(time.Second * 3)
			for j := 31; j <= 40; j++ {
				node2.Append("hai RATHEESH " + strconv.Itoa(j))
				node3.Append("hai RATHEESH " + strconv.Itoa(j))
			}
		}

	} else {

		err := Mcluster.Partition([]int{1, 2, 3}, []int{4, 5})

		if err != nil {
			t.Error("Mk Cluster ERROR :", err)
			time.Sleep(time.Second * 3)

			time.Sleep(time.Second * 3)
			for j := 31; j <= 40; j++ {
				node2.Append("hai RATHEESH " + strconv.Itoa(j))
				node5.Append("hai RATHEESH " + strconv.Itoa(j))
			}
		}

	}

	time.Sleep(time.Second * 3)

	Mcluster.Heal()

	time.Sleep(time.Second * 3)

	fmt.Println("7")

	for node1.sm.log.GetLastIndex() == 240 && node2.sm.log.GetLastIndex() == 240 && node3.sm.log.GetLastIndex() == 240 && node4.sm.log.GetLastIndex() == 240 && node5.sm.log.GetLastIndex() == 240 {
	}

	fmt.Println("8")
}

func TestNodeFailure(t *testing.T) {

	// Check for leader failure, and check for re-ellection

	list := []*RaftNode{node1, node2, node3, node4, node5}

	// Testing Sendng appaend request to leader directly
	id := 0
	for a := range list {
		if list[a].sm.state == "LEADER" {
			id = list[a].nodeId
			list[a].Shutdown()
			break
		}
	}

	var newlist []*RaftNode

	for a := range list {
		if list[a].nodeId != id {
			newlist = append(newlist, list[a])
		}
	}

	time.Sleep(time.Second * 2)
	//	fmt.Println("five")

	fmt.Println("9")
	for {
		if newlist[0].sm.state == "LEADER" || newlist[1].sm.state == "LEADER" || newlist[2].sm.state == "LEADER" || newlist[3].sm.state == "LEADER" {
			break
		}
	}

	//	fmt.Println("six")
	fmt.Println("10")
	leader = 0
	candidate = 0
	follower = 0

	for a := range newlist {
		if newlist[a].sm.state == "LEADER" {
			leader++
		} else if newlist[a].sm.state == "CANDIDATE" {
			candidate++
		} else if newlist[a].sm.state == "FOLLOWER" {
			follower++
		}
	}

	if leader > 1 {
		t.Error("\n More nodes elected as leader Expected leader count :1 Found :%d", leader)
	} else if follower < 3 && candidate > 0 {
		t.Error("\n Candiate count Expected : 3 Found :%d, Follower Count Expected : Found : %d", candidate, follower)
	}

	// Check for appending via leader

	//	fmt.Print(newlist[0].sm.log.GetLastIndex())
	for a := range newlist {
		if newlist[a].sm.state == "LEADER" {
			for j := 1; j <= 20; j++ {
				newlist[a].Append("Test Command " + strconv.Itoa(j))
			}
			break
		}
	}
	//	fmt.Println("three")

	time.Sleep(time.Second * 5)

	fmt.Println("11")
	for {
		if newlist[0].sm.log.GetLastIndex() == 260 && newlist[1].sm.log.GetLastIndex() == 260 && newlist[2].sm.log.GetLastIndex() == 260 && newlist[3].sm.log.GetLastIndex() == 260 {
			break
		}
	}

	fmt.Println("12")
	//	fmt.Println("four")

	// Checking second node failure

	for a := range newlist {
		if newlist[a].sm.state == "LEADER" {
			id = newlist[a].nodeId
			newlist[a].Shutdown()
			break
		}
	}

	var _newlist []*RaftNode

	for a := range newlist {
		if newlist[a].nodeId != id {
			_newlist = append(_newlist, newlist[a])
		}
	}

	time.Sleep(time.Second * 3)
	//	fmt.Println("five")
	fmt.Println("13")
	for {
		if _newlist[0].sm.state == "LEADER" || _newlist[1].sm.state == "LEADER" || _newlist[2].sm.state == "LEADER" {
			break
		}
	}

	fmt.Println("14")
	leader = 0
	candidate = 0
	follower = 0

	for a := range _newlist {
		if _newlist[a].sm.state == "LEADER" {
			leader++
		} else if _newlist[a].sm.state == "CANDIDATE" {
			candidate++
		} else if _newlist[a].sm.state == "FOLLOWER" {
			follower++
		}
	}

	for a := range _newlist {
		if _newlist[a].sm.state == "LEADER" {
			id = _newlist[a].nodeId
			_newlist[a].Shutdown()
			break
		}
	}

	var __newlist []*RaftNode

	for a := range _newlist {
		if _newlist[a].nodeId != id {
			__newlist = append(__newlist, _newlist[a])
		}
	}

	time.Sleep(time.Second * 3)
	//	fmt.Println("five")

	if __newlist[0].sm.state == "LEADER" || __newlist[1].sm.state == "LEADER" {
		t.Error("Leader cannot be elected")
	}

}

func ColorPrinting(nodeId int) {
	switch nodeId {
	case 1:
		//fmt.Println(CLR_R + "Sample Coloring" + CLR_END)
	case 2:
		//fmt.Println(CLR_G + "Sample Coloring" + CLR_END)
	case 3:
		//fmt.Println(CLR_Y + "Sample Coloring" + CLR_END)
	case 4:
		//fmt.Println(CLR_B + "Sample Coloring" + CLR_END)
	case 5:
		//fmt.Println(CLR_M + "Sample Coloring" + CLR_END)
	default:
	}
}
