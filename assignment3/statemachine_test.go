package sm

import (
	"fmt"
	"testing"

)


// Simple serial check of getting and setting
func TestStateMachine(t *testing.T) {
 	
	candidateTestcases(t)
	leaderTestcases(t)
	followerTestcases(t)
}

func candidateTestcases(t *testing.T){

	var response interface{}
	var event interface{}
	sm := StateMachine{
							id:1,
							state:"CANDIDATE",
							peers:[] int {2,3,4,5},
							currentTerm : 2, 
							logLastIndex:2,
							commitIndex:2,
							lastApplied:2}

	sm.votedFor = make(map[int]int)
	entry1 := Entry{1,"A"}
	sm.log[1] = entry1
	entry1 = Entry{2,"B"}
	sm.log[2] = entry1

//	testcase 1 : testing timeout
	val := sm.currentTerm 
	event = Timeout{}
	sm.ProcessEvent(event)

				if( val+1 != sm.currentTerm ){
					t.Error(fmt.Sprintf("Error : Election: Current Term NOt incremented"))
				}
				if(sm.votedFor[val+1] != sm.id){
					// Not voted to itself
					t.Error(fmt.Sprintf("Error : No Self vote"))
				}
	response = GetResponseEventFromChannel()
	response = GetResponseEventFromChannel()
	response = GetResponseEventFromChannel()
	response = GetResponseEventFromChannel()
// processing the last response
	cmd := response.(Send)
	switch cmd.id {
	case 2:
	case 3:
	case 4:
	case 5:
	default:
		t.Error(fmt.Sprintf("Expecting 2,3,4,5 as server ID"))
	}
	switch cmd.event.(type) {
	case VoteReqEv: // expecting vote request event type
		obj:= cmd.event.(VoteReqEv)
		if( obj.term != val+1 || obj.candidateId != sm.id ){
				t.Error(fmt.Sprintf("Error in leader state values"))
		}
	default:
		t.Error(fmt.Sprintf("Expecting type : VoteReqEv"))
	}


//	testcase 1 : Vote response event
//`	VoteResponse 1

		event = VoteRespEv{term:3 ,	voteGranted:true} // first response to vote request
		sm.ProcessEvent(event)
		//response = GetResponseEventFromChannel()

		if(sm.state != "CANDIDATE"){
			t.Error(fmt.Sprintf("Expecting 'CANDIDATE state':"))
		}

		event = VoteRespEv{term:3,	voteGranted:true} // 2nd response to vote request
		sm.ProcessEvent(event)

		if(sm.state != "CANDIDATE"){
			t.Error(fmt.Sprintf("Expecting 'CANDIDATE state':"))
		}

  		event = VoteRespEv{term:3 ,	voteGranted:true} // 3rd response to vote request:MAjoruty
		sm.ProcessEvent(event)

		if(sm.state != "LEADER"){      // checking the server become LEADER
			t.Error(fmt.Sprintf("Expecting 'LEADER state':"))
		}

	//	fmt.Print("\nSTATE3 ",sm.state)


	response = GetResponseEventFromChannel() // receive first heartbeat from new leader
	cmd = response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesReqEv:
			//	fmt.Print("\nSTATE ",response)
	default:
		t.Error(fmt.Sprintf("Expecting 'AppendEntriesReqEv' event:"))
	}


	response = GetResponseEventFromChannel() // receive heartbeat from new leader
	cmd = response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesReqEv:
			//	fmt.Print("\nSTATE ",response)
	default:
		t.Error(fmt.Sprintf("Expecting 'AppendEntriesReqEv' event:"))
	}

	response = GetResponseEventFromChannel() // receive   heartbeat from new leader
	cmd = response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesReqEv:
			//fmt.Print("\nSTATE ",response)
	default:
		t.Error(fmt.Sprintf("Expecting 'AppendEntriesReqEv' event:"))
	}

	response = GetResponseEventFromChannel() // receive   heartbeat from new leader
	cmd = response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesReqEv:
			//fmt.Print("\nSTATE ",response)
	default:
		t.Error(fmt.Sprintf("Expecting 'AppendEntriesReqEv' event:"))
	}

 	//fmt.Print("\nTERM : ",sm.currentTerm)

// sending a vote response evenet with lower term
		event = VoteRespEv{term:2 ,	voteGranted:true}
		sm.ProcessEvent(event)

		if(sm.state != "LEADER"){ // no change in state of server
			t.Error(fmt.Sprintf("Expecting 'LEADER' state':"))
		}

// sending a vote response evenet with higher term
 		event = VoteRespEv{term:4 ,	voteGranted:true}
		sm.ProcessEvent(event)

		if(sm.state != "FOLLOWER"){ // server become FOLLOWER
			t.Error(fmt.Sprintf("Expecting 'FOLLOWER' state':"))
		}



// sending a vote response evenet with higher term
		event = VoteRespEv{term:5 ,	voteGranted:true}
		sm.ProcessEvent(event)

		if(sm.state != "FOLLOWER"){
			t.Error(fmt.Sprintf("Expecting 'FOLLOWER' state':"))
		}
	





// Testing timeout event, state : FOLLOWER
	val = sm.currentTerm
	event = Timeout{}
	sm.ProcessEvent(event)

				if(sm.state != "CANDIDATE"){ // checking server become 'CANDIDATE'
				t.Error(fmt.Sprintf("Expecting 'CANDIDATE' state':"))
				}

				if( val+1 != sm.currentTerm ){ // server term number incremented as part of election
					t.Error(fmt.Sprintf("Error : Election: Current Term NOt incremented"))
				}
				if(sm.votedFor[val+1] != sm.id){ // server self voted as part of election
					// Not voted to itself
					t.Error(fmt.Sprintf("Error : No Self vote"))
				} 

	response = GetResponseEventFromChannel() //  vote request from CANDIDATE
	response = GetResponseEventFromChannel() //  vote request from CANDIDATE
	response = GetResponseEventFromChannel() //  vote request from CANDIDATE
	response = GetResponseEventFromChannel() //  vote request from CANDIDATE

// vote granted response from server with same term

		event = VoteRespEv{term:5 ,	voteGranted:true} 
		sm.ProcessEvent(event)

		if(sm.state != "CANDIDATE"){ // no change in candidate state
			t.Error(fmt.Sprintf("Expecting 'CANDIDATE' state':"))
		}

// vote denied response from server with higher term

		event = VoteRespEv{term:7 ,	voteGranted:false}
		sm.ProcessEvent(event)

		if(sm.state != "FOLLOWER"){ // candidate become follower
			t.Error(fmt.Sprintf("Expecting 'FOLLOWER' state':"))
		}

// APPEND REQUEST TO A FOLLOWER, WHO KNOW THE ACTUAL LEADER ID

	sm.leaderId = 3
	event = Append{data:"B"}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel() // redirect it to the leader with ID:3

	cmd= response.(Send)
	//fmt.Print(cmd)
	if(cmd.id  != 3 ){ 
		t.Error(fmt.Sprintf("Expecting 3 as leader ID"))
	}

	switch cmd.event.(type) { 
	case Append: // append event forwarded to leader
		obj:= cmd.event.(Append)
		if( obj.data != "B"){
				t.Error(fmt.Sprintf("Error in Append data values"))
		}
	default:
		t.Error(fmt.Sprintf("Error in msg type"))
	}

//CREATE A VOTE REQUEST WITH LOW TERM NUMBER 6, CURRENT TERM: 7
	event = VoteReqEv{term:6,candidateId:2,lastLogIndex:1,lastLogTerm:1}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()

// VOTE REQUEST REJECTED
	cmd= response.(Send)
	//fmt.Print(cmd)
	if(cmd.id  != 2 ){
		t.Error(fmt.Sprintf("Expecting 2 as cadidateID, who send vote request"))
	}
	switch cmd.event.(type) {
	case VoteRespEv:
			obj := cmd.event.(VoteRespEv)
			if(obj.term != sm.currentTerm){
					t.Error(fmt.Sprintf("Error : exepecting Term : ",sm.currentTerm," found :",obj.term," event"))			
				} 
			if(obj.voteGranted){
					t.Error(fmt.Sprintf("Error : Vote should not be granted"))			
			}
	default:
		t.Error(fmt.Sprintf("Error : exepecting 'VoteRespEv' event"))	
	}

	//fmt.Print(sm.state)

//CREATE A VOTE REQUEST WITH HIGHER TERM NUMBER 4, CURRENT TERM reset to  3
// CURRENT STATE : FOLLOWER


	sm.logLastIndex = 4
	sm.currentTerm  = 3


	event = VoteReqEv{term:4,candidateId:2,lastLogIndex:3,lastLogTerm:4}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()

// VOTE REQUEST REJECTED
	cmd= response.(Send)
	//fmt.Print(cmd)
	if(cmd.id  != 2 ){
		t.Error(fmt.Sprintf("Expecting 2 as cadidateID, who send vote request"))
	}
	switch cmd.event.(type) {
	case VoteRespEv:
			obj := cmd.event.(VoteRespEv)
			if(obj.term != 4){
					t.Error(fmt.Sprintf("Error : exepecting Term :4  found :",obj.term," event"))			
				} 
			if(sm.votedFor[sm.currentTerm] != 2){
				t.Error(fmt.Sprintf("Error : Not voted for candidate 2"))			
			}
			if(!obj.voteGranted){
					t.Error(fmt.Sprintf("Error : Vote should not be granted"))			
			}
	default:
		t.Error(fmt.Sprintf("Error : exepecting 'VoteRespEv' event"))	
	}


// VOTE REEQUEST FROM THE SAME TERM NUMBER

	event = VoteReqEv{term:4,candidateId:2,lastLogIndex:3,lastLogTerm:4}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()

	

// VOTE REQUEST FOR THE SAME TERM NUMBER WITH lower LOG CONTENT/LARGE INDEX (MORE UPTO DATE)
// state machiine lastindex :4
// same TERM : 5

	sm.currentTerm = 5
	sm.logLastIndex = 4
	sm.logLastTerm = 4

	event = VoteReqEv{term:5,candidateId:3,lastLogIndex:3,lastLogTerm:4}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()

// VOTE REQUEST REJECTED
	cmd= response.(Send)
	//fmt.Print(cmd)
	if(cmd.id  != 3 ){
		t.Error(fmt.Sprintf("Expecting 3 as cadidateID, who send vote request"))
	}
	switch cmd.event.(type) {
	case VoteRespEv:
			obj := cmd.event.(VoteRespEv)
			if(obj.term != 5){
					t.Error(fmt.Sprintf("Error : exepecting Term :5  found :",obj.term," event"))			
				} 
			if(sm.votedFor[sm.currentTerm] == 3){
				t.Error(fmt.Sprintf("Error : Should Not voted for candidate 3"))			
			}
			if(obj.voteGranted){
					t.Error(fmt.Sprintf("Error : Vote not granted"))			
			}
	default:
		t.Error(fmt.Sprintf("Error : exepecting 'VoteRespEv' event"))	
	}


 
// Current state : FOLLOWER
// Current Term  : 5
// TEst : Lower term AppendEntriesReqEv request
	sm.leaderId = 3
	event = AppendEntriesReqEv{term :4}
 	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()
	
// AppendEntry request REJECTED
	cmd= response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesRespEv:
		obj := cmd.event.(AppendEntriesRespEv)
		if(obj.success){
			t.Error(fmt.Sprintf("Expecting AppendEntriesRequest Failed"))
		}
		if(obj.term  == 4 ){
		t.Error(fmt.Sprintf("Expecting 5 as Term number"))
		}
	default:
		t.Error(fmt.Sprintf("Expecting AppendEntriesRespEv as type of response"))
	}

// SENDING heartbeat
	
	event = AppendEntriesReqEv{term:6,lengthOfLogEnty:0}
 	sm.ProcessEvent(event)

 	response = GetResponseEventFromChannel()
 	 switch response.(type){
 		case Alarm: 
	    default:
  			t.Error(fmt.Sprintf("Expected to set Am alarm for election timeout"))
	}

// CANDIADTE receive a AppendEntriesReqEv from higher term

entry := Entry{term:1,command:"A"}
sm.log[1] = entry
entry = Entry{term:1,command:"B"}
sm.log[2] = entry
entry = Entry{term:2,command:"C"}
sm.log[3] = entry
entry = Entry{term:2,command:"F"}
sm.log[4] = entry
sm.state = "CANDIDATE"
sm.currentTerm = 3
sm.logLastIndex = 4
sm.commitIndex = 3


// AppendEntriesReqEv Log Entries
logEntry := [10] Entry{}
logEntry[1] = Entry{term:1,command:"A"}
logEntry[2] = Entry{term:1,command:"B"}
logEntry[3] = Entry{term:2,command:"C"}
logEntry[4] = Entry{term:3,command:"D"}
logEntry[5] = Entry{term:4,command:"E"} 
logEntry[6] = Entry{term:5,command:"E"} // new append entry

// Append Enrty Request 

event = AppendEntriesReqEv{term:5,leaderId:5,prevLogIndex:5,prevLogTerm:4,logEntries:logEntry,lengthOfLogEnty:6,leaderCommit:5}
sm.ProcessEvent(event)

if (sm.state != "FOLLOWER"){
		t.Error(fmt.Sprintf("Expected state : FOLLOWER, found :",sm.state))
}
if(sm.leaderId != 5 ){
		t.Error(fmt.Sprintf("Expected leader ID : 5, found :",sm.leaderId))	
}

// AppendEntriesReqEv request rejected
//	var responseFrom int
	response = GetResponseEventFromChannel()
//	fmt.Print(response)
	cmd= response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesRespEv:
		obj := cmd.event.(AppendEntriesRespEv)
//		responseFrom = obj.id
		if(obj.success){
			t.Error(fmt.Sprintf("Expecting AppendEntriesRequest Failed"))
		}
		if(obj.term  != 5 ){
		t.Error(fmt.Sprintf("Expecting 5 as Term number"))
		}
	default:
//		responseFrom = 0
		t.Error(fmt.Sprintf("Expecting AppendEntriesRespEv as type of response"))
	}

// decrement the index and retry AppendEntriesReqEv

event = AppendEntriesReqEv{term:5,leaderId:5,prevLogIndex:4,prevLogTerm:4,logEntries:logEntry,lengthOfLogEnty:6,leaderCommit:5}
sm.ProcessEvent(event)

// expecting the AppendEntriesRespEv is failed

	response = GetResponseEventFromChannel()
//	fmt.Print(response)
	cmd= response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesRespEv:
		obj := cmd.event.(AppendEntriesRespEv)
//		responseFrom = obj.id
		if(obj.success){
			t.Error(fmt.Sprintf("Expecting AppendEntriesRequest Failed"))
		}
		if(obj.term  != 5 ){
		t.Error(fmt.Sprintf("Expecting 5 as Term number"))
		}
	default:
//		responseFrom = 0
		t.Error(fmt.Sprintf("Expecting AppendEntriesRespEv as type of response"))
	}

// decrement the index and retry AppendEntriesReqEv

event = AppendEntriesReqEv{term:5,leaderId:5,prevLogIndex:3,prevLogTerm:2,logEntries:logEntry,lengthOfLogEnty:6,leaderCommit:5}
sm.ProcessEvent(event)
	
	// log become identical to leader's log, who initiate AppendEntriesReqEv event
	for i:=1 ;i<=6;i++ {
		if(sm.log[i] != logEntry[i]){
				t.Error(fmt.Sprintf("Expecting : Leader array and client array are same"))
		}
	}
	
 	if(sm.logLastIndex != 6){
		t.Error(fmt.Sprintf("Expecting Log index not upto date")) 	
 	}

	response = GetResponseEventFromChannel()
//	fmt.Print(response)
	cmd= response.(Send)
	switch cmd.event.(type) {
	case AppendEntriesRespEv:
		obj := cmd.event.(AppendEntriesRespEv)
//		responseFrom = obj.id
		if(!obj.success){
			t.Error(fmt.Sprintf("Expecting AppendEntriesRequest succe"))
		}
		if(obj.term  != 5 ){
		t.Error(fmt.Sprintf("Expecting 5 as Term number"))
		}
	default:
//		responseFrom = 0
		t.Error(fmt.Sprintf("Expecting AppendEntriesRespEv as type of response"))
	}

// leader commit < log's last index, update statemachine commit index
	if(sm.commitIndex != 5){
		t.Error(fmt.Sprintf("Expecting state machine commit index updated"))
	}
 
// leader commit >= log's last index, update statemachine commit index
event = AppendEntriesReqEv{term:5,leaderId:5,prevLogIndex:3,prevLogTerm:2,logEntries:logEntry,lengthOfLogEnty:6,leaderCommit:6}
sm.ProcessEvent(event)
response = GetResponseEventFromChannel()

	if(sm.commitIndex != 6){
		t.Error(fmt.Sprintf("Expecting state machine commit index updated"))
	}

// Creating event 'AppendEntriesRespEv'
// state CANDIDATE
	sm.state = "CANDIDATE"
	sm.currentTerm = 4
	event = AppendEntriesRespEv{id:1, term:5, success:false}	
	sm.ProcessEvent(event)

	if(sm.state != "FOLLOWER"){
		t.Error(fmt.Sprintf("Expecting : FOLLOWER state, Found : CANDIDATE"))
	}
	if(sm.currentTerm == 4){
		t.Error(fmt.Sprintf("Expecting : Term : 5 ,Found : Term :4"))
	}



// Creating event 'AppendEntriesRespEv'
// state CANDIDATE
	sm.state = "FOLLOWER"
	sm.currentTerm = 4
	event = AppendEntriesRespEv{id:1, term:5, success:false}	
	sm.ProcessEvent(event)

	if(sm.currentTerm == 4){
		t.Error(fmt.Sprintf("Expecting : Term : 5 ,Found : Term :4"))
	}
}


func leaderTestcases(t *testing.T){

	var response interface{}
	var event interface{}
 

	sm := StateMachine{
							id:1,
							leaderId:1,
							state:"LEADER",
							peers:[] int {2,3,4,5},
							currentTerm : 1, 
							replicateCount:0,
							logLastIndex:0,
							commitIndex:0,
							lastApplied:0}

	sm.log[0] = Entry{term:0 ,command:""}
	sm.votedFor = make(map[int]int)
	sm.nextIndex  = []int{sm.logLastIndex+1,sm.logLastIndex+1,sm.logLastIndex+1,sm.logLastIndex+1,sm.logLastIndex+1}
	sm.matchedIndex = []int{0,0,0,0,0}

//	testcase 1

	event = Append{data:"A"}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()
 
	//fmt.Print("\nResponse : ",response.event)

	cmd:= response.(Send)

	switch cmd.id {
	case 2:
	case 3:
	case 4:
	case 5:
	default:
		t.Error(fmt.Sprintf("Expecting 2,3,4,5 as server ID"))
	}

	switch cmd.event.(type) {
	case AppendEntriesReqEv:
		obj:= cmd.event.(AppendEntriesReqEv)
		if( obj.term != sm.currentTerm || obj.leaderId != sm.id ||	obj.prevLogIndex != 0 || obj.prevLogTerm != 0){
				t.Error(fmt.Sprintf("Error in leader state values"))
		}

	if (obj.logEntries[0].term != sm.currentTerm){
		t.Error(fmt.Sprintf("Error in Term  number"))
		}

	if(obj.logEntries[0].command != "A" ) {
		t.Error(fmt.Sprintf("Error in Command"))
		}
	default:
		t.Error(fmt.Sprintf("Expecting type : AppendEntriesReqEv"))
	}

	if(sm.state!="LEADER" || sm.logLastIndex != 1 || sm.log[1].term !=1 || sm.log[1].command != "A"){
		t.Error(fmt.Sprintf("Error in leader state values"))
	}


// Apped request to all other peers
	response = GetResponseEventFromChannel()   
	response = GetResponseEventFromChannel()   
	response = GetResponseEventFromChannel()   

// TimeOUT Event to leader
// 
	event = Timeout{}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()	
	// 

cmd = response.(Send)

	switch cmd.id {
	case 2:
	case 3:
	case 4:
	case 5:
	default:
		t.Error(fmt.Sprintf("Expecting 2,3,4,5 as server ID"))
	}

	switch cmd.event.(type) {
	case AppendEntriesReqEv:
		obj:= cmd.event.(AppendEntriesReqEv)
		if(obj.lengthOfLogEnty !=0 ){
			t.Error(fmt.Sprintf("Not a heartbeat"))	
		}

	default:
		t.Error(fmt.Sprintf("Expecting type : AppendEntriesReqEv"))
	}

// heart beats events to all other peers
	response = GetResponseEventFromChannel()   
	response = GetResponseEventFromChannel()   
	response = GetResponseEventFromChannel()   
	//response = GetResponseEventFromChannel()   




// EVENT : 'AppendEntriesRespEv' to Leader

	sm.state = "LEADER"
	sm.id = 4
// higher term, and appendReqEv failure

	event = AppendEntriesRespEv{id:2,term:4,success:false}

	sm.ProcessEvent(event)
	if(sm.currentTerm != 4){
		t.Error(fmt.Sprintf("Expecting Term:4 found Term : %v",sm.currentTerm))
	}
	if(sm.state != "FOLLOWER"){
		t.Error(fmt.Sprintf("Expecting STATE:FOLLOWER, Found :%v",sm.state))
	}
	
// similar term, and appendReqEv  success, client with id replicate the data
	sm.replicateCount = 0 // no AppendReqResponse so far
	sm.state="LEADER"
	//fmt.Print(sm.currentTerm)
	event = AppendEntriesRespEv{id:2,term:4,success:true} // success fully appedned to log
	sm.ProcessEvent(event)

	event = AppendEntriesRespEv{id:1,term:4,success:true} //// success fully appedned to log
	sm.ProcessEvent(event)

	event = AppendEntriesRespEv{id:3,term:4,success:true} //// success fully appedned to log: MAJORITY
	sm.ProcessEvent(event)

	if (sm.replicateCount != 3){
		t.Error(fmt.Sprintf("Expecting: Majority of servers appedned the new log entry"))	
	}

	response = GetResponseEventFromChannel()	
	switch  response.(type) {
	case Commit:
		obj:=  response.(Commit)
		if(obj.index != sm.commitIndex){
			t.Error(fmt.Sprintf("Expecting: Commit index of leader not updated"))
		}

	default:
		t.Error(fmt.Sprintf("Expecting type : Commit , Not Found"))
	}


 

// Send AppendEntriesRespEv with matching term with leader but, the appedn request was a failure

	sm.nextIndex[2] = 2
	event = AppendEntriesRespEv{id:2,term:4,success:false}
	lastindx:=sm.logLastIndex
	sm.ProcessEvent(event)


	if (sm.nextIndex[2] == 2){
		t.Error(fmt.Sprintf("Expecting NextIndex as 1, found :2",))
	}
	response = GetResponseEventFromChannel()	
	cmd= response.(Send)

	switch cmd.event.(type) {
	case AppendEntriesReqEv:

			obj:=  cmd.event.(AppendEntriesReqEv)
			if(lastindx-1 != obj.prevLogIndex){ // check whether the prevLogIndex is decremented
				t.Error(fmt.Sprintf("Expecting prevLogIndex :%vtype , Found : %v",lastindx-1,obj.prevLogIndex))				
			}
	default:
		t.Error(fmt.Sprintf("Expecting type : AppendEntriesReqEv Not found!"))
	}

}




func followerTestcases(t *testing.T){

	var response interface{}
	var event interface{}
	sm := StateMachine{
							id:1,
							state:"FOLLOWER",
							peers:[] int {2,3,4,5},
							currentTerm : 1, 
							logLastIndex:0,
							commitIndex:0,
							lastApplied:0}

	sm.log[0] = Entry{term:0 ,command:""}
	sm.votedFor = make(map[int]int)
	sm.leaderId = 5

//	testcase 1 : Testing Append Event

	event = Append{data:"A"}
	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()

	cmd:= response.(Send)

	//fmt.Print(cmd)
	if(cmd.id  != 5 ){
		t.Error(fmt.Sprintf("Expecting 5 as leader ID"))
	}

	switch cmd.event.(type) {
	case Append:
		obj:= cmd.event.(Append)
		if( obj.data != "A"){
				t.Error(fmt.Sprintf("Error in leader state values"))
		}
	default:
		t.Error(fmt.Sprintf("Error in leader state values"))
	}

// Testcase 2 : testing timeout

	val := sm.currentTerm 
	event = Timeout{}
	sm.ProcessEvent(event)

				if( val+1 != sm.currentTerm ){
					t.Error(fmt.Sprintf("Error : Election: Current Term NOt incremented"))
				}

				if(sm.state != "CANDIDATE"){
					t.Error(fmt.Sprintf("Error : State is not changed during election"))
				}

				if(sm.votedFor[sm.currentTerm] != sm.id){
					// Not voted to itself
					t.Error(fmt.Sprintf("Error : No Self vote"))
				}

	response = GetResponseEventFromChannel()
	cmd = response.(Send)

	switch cmd.id {
	case 2:
	case 3:
	case 4:
	case 5:
	default:
		t.Error(fmt.Sprintf("Expecting 2,3,4,5 as server ID"))
	}


	switch cmd.event.(type) {
	case VoteReqEv:
		obj:= cmd.event.(VoteReqEv)
		if( obj.term != val+1 || obj.candidateId != sm.id ){
				t.Error(fmt.Sprintf("Error in leader state values"))
		}
	default:
		t.Error(fmt.Sprintf("Expecting type : VoteReqEv"))
	}

}
 


 
