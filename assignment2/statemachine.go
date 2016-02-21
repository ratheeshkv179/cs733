package sm
import (
		"fmt"
	   )

var eventChannel = make(chan interface{},5)
var actionChannel = make(chan interface{},5)
var clientChannel  = make (chan interface{},5)

type VoteReqEv struct {

	term int
	candidateId int
	lastLogIndex int
	lastLogTerm int
}

type VoteRespEv struct {

	term int
	voteGranted bool
}

type Entry struct {

	term int
	command string
}

type Timeout struct {

}

type Append struct {

	data [1] byte
}

type Commit struct {
	index int
	data [] byte
	err string
}

type Alarm struct {

}


type Send struct {
	id int
	event interface{}
}
type Logstore struct {
	index int
	data [] byte

}

type AppendEntriesReqEv struct {

	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	logEntries[100] byte
	leaderCommit int
	// etc
}

type AppendEntriesRespEv struct{

	term int
	success bool	
}


type StateMachine struct {

	id int // server id
	leaderId int // leader id of the cluster
	state string // state of stateMachine LEADER, CANDIDATE or FOLLOWER
	peers []int  // other server ids
	currentTerm int 
	votedFor int
	voteCount int
	replicateCount int
	log[100] byte
	logLastIndex int
	logLastTerm int
	commitIndex int
	lastApplied int
	nextIndex[] int
	matchedIndex[] int
	// etc
}

 
func PutEventToChannel( ev interface{}){
	//eventChannel <- ev
	
	switch ev.(type) {

		case Commit:
			clientChannel <- ev
		default:
			eventChannel  <- ev			
		}
}

func (sm *StateMachine) RestartStateMachine(id int){
	sm.state = "FOLLOWER"
}

func  GetResponseEventFromChannel() interface{}{
	
	var msg interface{}
	select {
     case msg := <- clientChannel:
	   	return msg
     case msg := <- eventChannel:
     	 return msg
	 }
	 return msg
}

func (sm *StateMachine) ProcessEvent (ev interface{}) {

	switch ev.(type) {
	case Append:
		cmd := ev.(Append)
		
		if(sm.state == "LEADER"){
			// append to log and send append request to all
			sm.logLastIndex = sm.logLastIndex + 1
			sm.log[sm.logLastIndex] = cmd.data[0]
			sm.voteCount = 0

			   event := AppendEntriesReqEv{
						term:sm.currentTerm,
						leaderId:sm.id,
						prevLogIndex:sm.logLastIndex-1,
						prevLogTerm:0,
						logEntries:sm.log,
						leaderCommit:sm.commitIndex}

			for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}
 
			}else if(sm.state == "CANDIDATE" || sm.state == "FOLLOWER"){
				// redirect to leader
				if(sm.leaderId != 0){
					go PutEventToChannel(Send{sm.leaderId,cmd})					
				}
			}


	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)

		if(sm.state == "LEADER"){

			if(cmd.term > sm.currentTerm){ // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term  
 				// append entries to the log
 				// recursively update the log
				actionChannel  <- AppendEntriesRespEv{ term:sm.currentTerm, success : true}
			} else if(cmd.term == sm.currentTerm){

			} else {

				// reject the request
				actionChannel  <- AppendEntriesRespEv{ term:sm.currentTerm, success : false}

			}

			}else if(sm.state == "CANDIDATE"){
				if(cmd.term > sm.currentTerm){
					sm.state = "FOLLOWER"
					sm.currentTerm = cmd.term  
 					// append entries to the log
 					// recursively update the log

			/*		if(sm.logLastIndex<cmd.prevLogIndex || ((sm.logLastIndex == cmd.prevLogIndex) && (sm.logLastTerm != cmd.prevLogTerm)){
						go PutEventToChannel(AppendEntriesRespEv{ term:sm.currentTerm, success : false})						
					}
			*/





				}else{
					// reject the request
				//	 response = GetResponseEventFromChannel()   			
					//fmt.Print("\nRES#",response,"#\n")
				//ionChannel  <- AppendEntriesRespEv{ term:sm.currentTerm, success : false}
				}
				// heartbeat 
				if(len(cmd.logEntries) == 0){
					//refresh the timer
				}



			}else if (sm.state == "FOLLOWER"){


			if(cmd.term > sm.currentTerm){ // new leader/ leader with higher term exist
				sm.currentTerm = cmd.term 
			}

			// check for heartbeat

			}




	case AppendEntriesRespEv:
		cmd := ev.(AppendEntriesRespEv)

		if(sm.state == "LEADER"){

			if(cmd.term > sm.currentTerm){ // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term  
			}else if(cmd.term < sm.currentTerm){
					// out of date packet or ignore
			}else {
				if(cmd.success){ //
					// increment count of majority
					sm.replicateCount = sm.replicateCount + 1
					if(sm.replicateCount >= 3) {		
					if(sm.commitIndex < sm.logLastIndex) {
						sm.commitIndex = sm.logLastIndex
						event := Commit {index:sm.commitIndex, data:sm.log[sm.logLastIndex:] , err:""}
						go PutEventToChannel(event)
					}			
					}
			}
			}
			

		}else if(sm.state == "CANDIDATE"){

				if(cmd.term > sm.currentTerm){ // new leader/ leader with higher term exist
					sm.state = "FOLLOWER"
					sm.currentTerm = cmd.term  
				}

		}else if (sm.state == "FOLLOWER"){

				if(cmd.term > sm.currentTerm){ // new leader/ leader with higher term exist
					sm.currentTerm = cmd.term  
				}
		}


	case VoteReqEv:
		cmd := ev.(VoteReqEv)
		fmt.Print("\n",cmd.term, sm.currentTerm)

			if(cmd.term < sm.currentTerm){
				go PutEventToChannel(VoteRespEv{term : sm.currentTerm , voteGranted: false})
			}else {

			if(cmd.term > sm.currentTerm){
				sm.currentTerm = cmd.term
				sm.votedFor = 0
			}

			if(sm.votedFor == 0 || sm.votedFor == sm.id){
				if((sm.logLastTerm  > cmd.lastLogTerm) ||
					(sm.logLastTerm  == cmd.lastLogTerm && sm.logLastIndex > cmd.lastLogIndex)){
					go PutEventToChannel(VoteRespEv{term : sm.currentTerm , voteGranted: false})		
				}else{
					sm.votedFor = cmd.candidateId
					go PutEventToChannel(VoteRespEv{term : sm.currentTerm , voteGranted: true})		
				}
			}else {
					go PutEventToChannel(VoteRespEv{term : sm.currentTerm , voteGranted: false})		
				  }
			}


	case VoteRespEv:
		cmd := ev.(VoteRespEv)

		if(sm.state == "LEADER"){

			if(cmd.term > sm.currentTerm){ // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term  
			}
		}else if(sm.state == "CANDIDATE"){

			if(cmd.term > sm.currentTerm){ // new leader/ leader with higher term exist
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term  
			} else	if(cmd.voteGranted){
			sm.voteCount = sm.voteCount + 1 // increment votecount
			if(sm.voteCount >=3){
				sm.state = "LEADER"
				// send heartbeat msg
				// for all other servers
			   event := AppendEntriesReqEv{
						term:sm.currentTerm,
						leaderId:sm.id,
						prevLogIndex:sm.logLastIndex-1,
						prevLogTerm:sm.logLastTerm,
						logEntries:[100] byte{},
						leaderCommit:sm.commitIndex}

			for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}
				}
			}
		}else if (sm.state == "FOLLOWER"){
			if(cmd.term > sm.currentTerm){ // update the term
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term  
			}		
		}


	case Timeout:

		if(sm.state == "LEADER"){
 
			   event := AppendEntriesReqEv{
						term:sm.currentTerm,
						leaderId:sm.id,
						prevLogIndex:sm.logLastIndex-1,
						prevLogTerm:0,
						logEntries:[100] byte{},
						leaderCommit:sm.commitIndex}

			for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}

			}else if(sm.state == "CANDIDATE"){
				// election timeut
				sm.currentTerm = sm.currentTerm + 1
				//sm.state = "CANDIADTE"
				sm.votedFor = sm.id

			   event := VoteReqEv{
			   					term:sm.currentTerm,
			   					candidateId:sm.id,
			   					lastLogIndex:sm.logLastIndex,
			   					lastLogTerm:sm.logLastTerm}

			for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}

			}else if(sm.state == "FOLLOWER"){
				// election timeout
				sm.currentTerm = sm.currentTerm + 1
				sm.state = "CANDIDATE"
				sm.votedFor = sm.id

				event := AppendEntriesReqEv{
						term:sm.currentTerm,
						leaderId:sm.id,
						prevLogIndex:sm.logLastIndex-1,
						prevLogTerm:0,
						logEntries:[100] byte{}, // empty data entry
						leaderCommit:sm.commitIndex}

				for i:= 1; i<= len(sm.peers);i++ {
					go PutEventToChannel(Send{sm.peers[i-1],event})
				}
				// to all other servers
				//actionChannel <- VoteReqEv{}
			}

		//fmt.Printf("%v\n", cmd)

	default: println ("Unrecognized")
	}
}

