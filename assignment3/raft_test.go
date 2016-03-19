package raft

import (
     "fmt"
        "testing"
                "github.com/cs733-iitb/cluster"
                "time"
)

const CLR_R = "\x1b[31;1m"
const CLR_G = "\x1b[32;1m"
const CLR_Y = "\x1b[33;1m"
const CLR_B = "\x1b[34;1m"
const CLR_M = "\x1b[35;1m"
const CLR_END = "\x1b[0m"



func TestStateMachine(t *testing.T) {
 
        //t.Fatal("Got different data")
       config1 := Config{
        cluster :[]NetConfig{{Id:1},{Id:2},{Id:3},{Id:4},{Id:5}},
        Id : 1,     
        LogDir : "/one",
        ElectionTimeout : 200,
        HeartbeatTimeout : 500}

        config2 := Config{
        cluster :[]NetConfig{{Id:1},{Id:2},{Id:3},{Id:4},{Id:5}},
        Id : 2,     
        LogDir : "/two",
        ElectionTimeout : 200,
        HeartbeatTimeout : 500}


        config3 := Config{
        cluster :[]NetConfig{{Id:1},{Id:2},{Id:3},{Id:4},{Id:5}},
        Id : 3,     
        LogDir : "/three",
        ElectionTimeout : 200,
        HeartbeatTimeout : 500}

        config4 := Config{
        cluster :[]NetConfig{{Id:1},{Id:2},{Id:3},{Id:4},{Id:5}},
        Id : 4,     
        LogDir : "/four",
        ElectionTimeout : 200,
        HeartbeatTimeout : 500}

        config5 := Config{
        cluster :[]NetConfig{{Id:1},{Id:2},{Id:3},{Id:4},{Id:5}},
        Id : 5,     
        LogDir : "/five",
        ElectionTimeout : 200,
        HeartbeatTimeout : 500}

 
        clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
                {Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
        }}

 
        InitiateCluster(clconfig)
        //InitiateCluster()

        node1 := New(config1)
        fmt.Print("\n Node 1 : ",node1)
        node2 := New(config2)
        fmt.Print("\n Node 2 : ",node2)
        node3 := New(config3)
        fmt.Print("\n Node 3 : ",node3)
        node4 := New(config4)
        fmt.Print("\n Node 4 : ",node4)
        node5 := New(config5)
        fmt.Print("\n Node 5 : ",node5)

        node1.Start()
        node2.Start()
        node3.Start()
        node4.Start()
        node5.Start()


        
        fmt.Print("\n1 = ",node1.sm.state)
       // ColorPrinting(node1.nodeId)
        fmt.Print("\n2 = ",node2.sm.state)
       // ColorPrinting(node2.nodeId)
        fmt.Print("\n3 = ",node3.sm.state)
       // ColorPrinting(node3.nodeId)
        fmt.Print("\n4 = ",node4.sm.state)
       //ColorPrinting(node4.nodeId)
        fmt.Print("\n5 = ",node5.sm.state)
       //ColorPrinting(node5.nodeId)

        time.Sleep(time.Second * 10)
        
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




        


