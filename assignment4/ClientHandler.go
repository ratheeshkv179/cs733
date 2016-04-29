package main

import (
	"bufio"
	"fmt"
	"github.com/ratheeshkv179/cs733/assignment5/fs"
	"github.com/ratheeshkv179/cs733/assignment5/raft"
	"net"
	"os"
	"strconv"
)

var client_id int = 0
var crlf = []byte{'\r', '\n'}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func reply(conn *net.TCPConn, msg *fs.Msg) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil

}

// The client Handler passes the request to the leader
func sendToRaftNode(msg *fs.Msg) {

	// node list
	list := []*raft.RaftNode{raft.Node1,
		raft.Node2,
		raft.Node3,
		raft.Node4,
		raft.Node5}

	// send appaend to leader only
	for i := range list {
		if list[i].Sm.State == "LEADER" { //} || list[i].Sm.State == "FOLLOWER" {
			list[i].Append(msg)
			break
		}
	}
}

func serve(conn *net.TCPConn, id int) {
	reader := bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr := fs.GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				conn.Close()
				break
			}
		}

		var response *fs.Msg

		// Identify the type of event
		switch msg.Kind {

		case 'r':
			msg.ClientId = id             // update with client id
			response = fs.ProcessMsg(msg) // process msg

		case 'w':
			msg.ClientId = id
			sendToRaftNode(msg)                   // send the message as append to Raft node
			ev := <-raft.ClientHandlerChannel[id] // wait for commit of previous request

			if ev.Err == nil {
				response = fs.ProcessMsg(msg) // If comitted allow the FS to process event
			} else {
				response.Kind = 73 // If not committed, INTERNAL ERROR notification
			}

		case 'c':
			msg.ClientId = id
			sendToRaftNode(msg)                   // send the message as append to Raft node
			ev := <-raft.ClientHandlerChannel[id] // wait for commit of previous request
			if ev.Err == nil {
				response = fs.ProcessMsg(msg) // If comitted allow the FS to process event
			} else {
				response.Kind = 73 // If not committed, INTERNAL ERROR notification
			}
		case 'd':
			msg.ClientId = id
			sendToRaftNode(msg)                   // send the message as append to Raft node
			ev := <-raft.ClientHandlerChannel[id] // wait for commit of previous request
			if ev.Err == nil {
				response = fs.ProcessMsg(msg) // If comitted allow the FS to process event
			} else {
				response.Kind = 73 // If not committed, INTERNAL ERROR notification
			}
		}

		if !reply(conn, response) {
			conn.Close()
			break
		}
	}
}

func serverMain() {

	// indicate number of client in each testcases
	client_id = 0
	// initiate the client channels
	InitChannel()
	// initiate the Raft cluster of nodes
	raft.InitSystem()

	tcpaddr, err := net.ResolveTCPAddr("tcp", "localhost:9000")
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr) // wait for client to connect
	check(err)

	for {
		tcp_conn, err := tcp_acceptor.AcceptTCP() // accept client request
		check(err)
		client_id++                   // incr client Id, since new client connected
		go serve(tcp_conn, client_id) // serve the client
	}
}

// Initialize the client handler channels
func InitChannel() {
	for i := range raft.ClientHandlerChannel {
		raft.ClientHandlerChannel[i] = make(chan raft.CommitInfo)
	}
}

func main() {

	serverMain()
}
