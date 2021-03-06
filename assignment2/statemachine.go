package sm
import (
 //"fmt"
	   )

var eventChannel = make(chan interface{},5)  // all events  timeout, vote req.reply, append req/rply pushed here
var actionChannel = make(chan interface{},5) // response events from server machine pushed here
var clientChannel  = make (chan interface{},5) // append data, commit pushed here

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
	data string
}

type Commit struct {
	index int
	data [] Entry
	err string
}

type Alarm struct {
	t int // time after t millisec, timeout will generated
}

type Send struct {
	id int
	event interface{}
}

type Logstore struct {
	index int
	data [10] Entry

}

type AppendEntriesReqEv struct {
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	logEntries[10] Entry
	lengthOfLogEnty int // length of log entry 
	leaderCommit int
}

type AppendEntriesRespEv struct{

	id int // serverID who generated this response
	term int // term numner
	success bool	// 
}

type StateMachine struct {

	id int 		 // this server id
	leaderId int // leader id of the cluster
	state string // state of stateMachine LEADER, CANDIDATE or FOLLOWER
	peers []int  // other server ids
	currentTerm int 
	votedFor map[int] int//make(map[int]int) // candidateID vs voted or not
	voteCount [10]int
	replicateCount int
	log[10] Entry
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

 
func  GetResponseEventFromChannel() interface{}{
	select {
     case msg := <- clientChannel: 
	   	return msg
     case msg := <- eventChannel:
     	 return msg
	 }
	 //msg = nil
}

func (sm *StateMachine) ProcessEvent (ev interface{}) {

	switch ev.(type) {
	case Append:
		cmd := ev.(Append)

		if(sm.state == "LEADER"){
			// append to log and send append request to all
			sm.logLastIndex = sm.logLastIndex + 1 // increment log index
			sm.log[sm.logLastIndex] = Entry{term:sm.currentTerm,command:cmd.data} // append to leader's log


			   event := AppendEntriesReqEv{
						term:sm.currentTerm,
						leaderId:sm.id,
						prevLogIndex:sm.logLastIndex-1,
						prevLogTerm:sm.log[sm.logLastIndex-1].term,
					//	logEntries:sm.log[sm.logLastIndex:],
						leaderCommit:sm.commitIndex}

				copy(event.logEntries[:],sm.log[sm.logLastIndex:])

			for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}

 
			}else if(sm.state == "FOLLOWER"){
				// redirect to leader
				if(sm.leaderId != 0){               // know the leaderID
					go PutEventToChannel(Send{sm.leaderId,cmd})					
				}
			}


	case AppendEntriesReqEv:
		cmd := ev.(AppendEntriesReqEv)

		// my term is higher
		if(cmd.term < sm.currentTerm){
			   event := AppendEntriesRespEv{
						term:sm.currentTerm,
						success:false}
			   go PutEventToChannel(Send{cmd.leaderId,event})
			   break
		} 

		if(cmd.lengthOfLogEnty == 0) {
			// heart beat from leader
			//refresh the election timeout
			// create an alarm, which Timeout after t milliseconds.
			event := Alarm{t:200}
			go PutEventToChannel(event)

		} else {

		if( sm.currentTerm < cmd.term){
			sm.currentTerm = cmd.term
			sm.state = "FOLLOWER"
		}

		// not a heartbeat, it is an actuall append request
		sm.leaderId = cmd.leaderId // got leaderID
		// no entry for particular index, my log is old
		if (sm.logLastIndex < cmd.prevLogIndex) {
			   event := AppendEntriesRespEv{
			   			id:sm.id,
						term:sm.currentTerm,
						success:false}
			   go PutEventToChannel(Send{cmd.leaderId,event})
			   break
		}

		// no matching entries in log (matching index and term)
		if ((sm.logLastIndex >= cmd.prevLogIndex) && (sm.log[cmd.prevLogIndex].term != cmd.prevLogTerm)){
			   			event := AppendEntriesRespEv{
			   			id:sm.id,
						term:sm.currentTerm,
						success:false}
			   go PutEventToChannel(Send{cmd.leaderId,event})
			   break
		} 
		// conflict with new entry
		if ((sm.logLastIndex >= (cmd.prevLogIndex+1)) && (sm.log[(cmd.prevLogIndex)+1].term != cmd.logEntries[0].term)){
			   			sm.logLastIndex = cmd.prevLogIndex // ignore all conflicting entries		   
		}
		// append new entries 
		if ((sm.logLastIndex == cmd.prevLogIndex) && (sm.log[sm.logLastIndex].term == cmd.prevLogTerm)){
			   			for i:= sm.logLastIndex+1 ;i<=cmd.lengthOfLogEnty ;i++ {
//			   				fmt.Print("ONE",cmd.logEntries[i])
			   				sm.log[i] = cmd.logEntries[i]
			   			}
			   			sm.logLastIndex = sm.logLastIndex + (cmd.lengthOfLogEnty-sm.logLastIndex)
			   			event := AppendEntriesRespEv{
			   							id:sm.id,
										term:sm.currentTerm,
										success:true}
			   			go PutEventToChannel(Send{cmd.leaderId,event})
			   			
		}

		if(cmd.leaderCommit > sm.commitIndex) {
			if( cmd.leaderCommit < sm.logLastIndex){
				sm.commitIndex = cmd.leaderCommit 
			} else {
				sm.commitIndex = sm.logLastIndex 
			}
		}	
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
					if(sm.replicateCount == 3) {	

					if(sm.commitIndex < sm.logLastIndex) { // got majority vote from peers to commit the data

						sm.commitIndex = sm.logLastIndex  // update commit index
						sm.nextIndex[cmd.id] = sm.logLastIndex+1
						sm.matchedIndex[cmd.id] = sm.logLastIndex


						event := Commit {index:sm.commitIndex, data:sm.log[:sm.logLastIndex] , err:""}
						go PutEventToChannel(event)
					}}
				}else { // log nconsistency, retry AppenedEntryRPC request 

						sm.nextIndex[cmd.id] = sm.nextIndex[cmd.id] - 1
						event:= AppendEntriesReqEv{
									term:sm.currentTerm,
									leaderId:sm.id,
									prevLogIndex:sm.logLastIndex-1,
									prevLogTerm:sm.log[sm.logLastIndex-1].term,
									lengthOfLogEnty:1,
									leaderCommit:sm.commitIndex}

							go PutEventToChannel(Send{cmd.id,event})

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
		//fmt.Print("\n",cmd.term, sm.currentTerm)

			if(cmd.term < sm.currentTerm){
				go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: false}})
			}else {

			if(cmd.term > sm.currentTerm){
				sm.currentTerm = cmd.term
				sm.state = "FOLLOWER"
				sm.votedFor[cmd.term] = 0 // first time seeing, not voted
			}

			if(sm.votedFor[cmd.term] == 0 || sm.votedFor[cmd.term] == sm.id){
				if((sm.logLastTerm  > cmd.lastLogTerm) ||
					(sm.logLastTerm  == cmd.lastLogTerm && sm.logLastIndex > cmd.lastLogIndex)){
					go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: false}})		
				}else{
					sm.votedFor[cmd.term] = cmd.candidateId //voted
					go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: true}})		
				}
			}else { // already voted for this term
					go PutEventToChannel(Send{cmd.candidateId,VoteRespEv{term : sm.currentTerm , voteGranted: false}})		
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

			} else if(cmd.term < sm.currentTerm){
				// ignore the msg packet
			}else {
				if(cmd.voteGranted){
					sm.voteCount[sm.currentTerm] = sm.voteCount[sm.currentTerm] + 1 // increment votecount
				
					if(sm.voteCount[sm.currentTerm] == 3){
						sm.state = "LEADER"
						// send heartbeat msg
						// for all other servers
			    		event := AppendEntriesReqEv{
									term:sm.currentTerm,
									leaderId:sm.id,
									prevLogIndex:sm.logLastIndex-1,
									prevLogTerm:sm.log[sm.logLastIndex-1].term,
									lengthOfLogEnty:0,
									leaderCommit:sm.commitIndex}

						for i:= 1; i<= len(sm.peers);i++ {
						//	fmt.Print("\nsending : ",i+1,event)
							go PutEventToChannel(Send{sm.peers[i-1],event})
						}
					}
			}}
		}else if (sm.state == "FOLLOWER"){
			if(cmd.term > sm.currentTerm){ // update the term
				sm.state = "FOLLOWER"
				sm.currentTerm = cmd.term  
			}		
		}


	case Timeout:

		if(sm.state == "LEADER"){
 				//heartbeat timeout	
			   event := AppendEntriesReqEv{
						term:sm.currentTerm,
						leaderId:sm.id,
						prevLogIndex:sm.logLastIndex-1,
						prevLogTerm:sm.log[sm.logLastIndex-1].term,
						lengthOfLogEnty:0,
						//logEntries:[100] byte{},
						leaderCommit:sm.commitIndex}

			for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}

			}else if(sm.state == "CANDIDATE"){
				// election timeut
				sm.currentTerm = sm.currentTerm + 1
				//sm.state = "CANDIADTE"
				sm.votedFor[sm.currentTerm] = sm.id
				sm.voteCount[sm.currentTerm] = 0

			   	event := VoteReqEv{
			   					term:sm.currentTerm,
			   					candidateId:sm.id,
			   					lastLogIndex:sm.logLastIndex,
			   					lastLogTerm:sm.log[sm.logLastIndex].term}

			for i:= 1; i<= len(sm.peers);i++ {
				go PutEventToChannel(Send{sm.peers[i-1],event})
			}

			}else if(sm.state == "FOLLOWER"){
				// election timeout
				sm.currentTerm = sm.currentTerm + 1
				sm.state = "CANDIDATE"
				sm.votedFor[sm.currentTerm] = sm.id
				sm.voteCount[sm.currentTerm] = 0

				
				event := VoteReqEv{
			   					term:sm.currentTerm,
			   					candidateId:sm.id,
			   					lastLogIndex:sm.logLastIndex,
			   					lastLogTerm:sm.log[sm.logLastIndex].term}

				for i:= 1; i<= len(sm.peers);i++ {
					go PutEventToChannel(Send{sm.peers[i-1],event})
				}
			}
	}
}

