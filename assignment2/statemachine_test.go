package sm

import (
	"fmt"
	"testing"

)


// Simple serial check of getting and setting
func TestStateMachine(t *testing.T) {
 
	leaderTestcases(t)
	candidateTestcases(t)
	followerTestcases(t)
	//expect(t,a,b)
}



func followerTestcases(t *testing.T){

//	var response interface{}
	var event interface{}
	var response interface{}
	_id := 1
	_state := "FOLLOWER"
	_peers := [] int {2,3,4,5}// other server ids
	_currentTerm  := 1
	_votedFor := 0
	_log := [100] byte{}
	_commitIndex := 0
	_lastApplied := 0
 
	sm := StateMachine{id : _id,
						state : _state,
						peers : _peers,
						currentTerm : _currentTerm ,
						votedFor: _votedFor,
						log: _log,
						commitIndex : _commitIndex,
						lastApplied : _lastApplied}

	
	event = Timeout{}
	sm.ProcessEvent(event)

	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	//2fmt.Print("\nRES@",sm.votedFor,"#\n")


	var command [1] byte 
    copy(command[:], "B")
//  fmt.Print("\nCOMMAND : ",command,len(command),"\n")
    sm.leaderId = 4
    event = Append{data:command}		
    sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nReDirect : @",response,"#\n")


	fmt.Print("\nTerm   ",sm.currentTerm)
	fmt.Print("\nLaIdx  ",sm.logLastIndex)
	fmt.Print("\nLaterm ",sm.logLastTerm)

	event = VoteReqEv{
					term:2,
					candidateId:3,
					lastLogIndex:0,
					lastLogTerm:0}


	sm.ProcessEvent(event)

	fmt.Print("\nTerm   ",sm.currentTerm)
	fmt.Print("\nLaIdx  ",sm.logLastIndex)
	fmt.Print("\nLaterm ",sm.logLastTerm)

		event = VoteReqEv{
					term:3,
					candidateId:3,
					lastLogIndex:2,
					lastLogTerm:1}


	sm.ProcessEvent(event)

	response = GetResponseEventFromChannel()   			
	fmt.Print("\nVotereq : ",response,"#\n")
 

	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)
	event = AppendEntriesRespEv{
							term:4,
							success:true}
	sm.ProcessEvent(event)						
	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)

	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)


	event = VoteRespEv{ term:2,
						voteGranted:true}
	sm.ProcessEvent(event)						
	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)


}


func candidateTestcases(t *testing.T){

	var response interface{}
	var event interface{}

	_id := 1
	_state := "CANDIDATE"
	_peers := [] int {2,3,4,5}// other server ids
	_currentTerm  := 1
	_votedFor := 1
	_log := [100] byte{}
	_logLastIndex := 0
	_commitIndex := 0
	_lastApplied := 0
	
	
	sm := StateMachine{id : _id,
						//leaderId:2,
						state : _state,
						peers : _peers,
						currentTerm : _currentTerm ,
						votedFor: _votedFor,
						voteCount:0,
						log: _log,
						logLastIndex : _logLastIndex,
						commitIndex : _commitIndex,
						lastApplied : _lastApplied}
	
	event = Timeout{}
	sm.ProcessEvent(event)

	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	//2fmt.Print("\nRES@",sm.votedFor,"#\n")


	var command [1] byte 
    copy(command[:], "B")
//  fmt.Print("\nCOMMAND : ",command,len(command),"\n")
    sm.leaderId = 4
    event = Append{data:command}		
    sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nReDirect : @",response,"#\n")

	event = VoteReqEv{
					term:1,
					candidateId:3,
					lastLogIndex:1,
					lastLogTerm:1}


	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nVotereq : ",response,"#\n")


	fmt.Print("\nTerm   ",sm.currentTerm)
	fmt.Print("\nLaIdx  ",sm.logLastIndex)
	fmt.Print("\nLaterm ",sm.logLastTerm)

	event = VoteReqEv{
					term:2,
					candidateId:3,
					lastLogIndex:0,
					lastLogTerm:0}

	sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nVotereQ : ",response,"#\n")

	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)
	event = AppendEntriesRespEv{
							term:2,
							success:true}
	sm.ProcessEvent(event)						
	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)

	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)

	event = VoteRespEv{ term:2,
						voteGranted:true}
	sm.ProcessEvent(event)						
	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)


	event = VoteRespEv{ term:2,
						voteGranted:true}
	sm.ProcessEvent(event)						
	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)

	event = VoteRespEv{ term:2,
						voteGranted:true}
	sm.ProcessEvent(event)						
	fmt.Print("\nTerm   ",sm.currentTerm,"state :",sm.state)


/*	event = AppendEntriesReqEv{
						term:2,
						leaderId:2,
						prevLogIndex:1,
						prevLogTerm:1,
						///logEntries:command[:],
						leaderCommit:1}		

    sm.ProcessEvent(event)
*/

	}
 






func leaderTestcases(t *testing.T){

	var response interface{}
	var event interface{}

	_id := 1
	_leaderId := 1
	_state := "LEADER"
	_peers := [] int {2,3,4,5}// other server ids
	_currentTerm  := 1
	_votedFor := 0
	_log := [100] byte{}
	_logLastIndex := 0
	_commitIndex := 0
	_lastApplied := 0
	_nextIndex := [] int {len(_log) + 1, len(_log) + 1, len(_log) + 1, len(_log) + 1}
	_matchedIndex := [] int {0,0,0,0}

	sm := StateMachine{id : _id,
						leaderId: _leaderId,
						state : _state,
						peers : _peers,
						currentTerm : _currentTerm ,
						votedFor: _votedFor,
						log: _log,
						logLastIndex : _logLastIndex,
						commitIndex : _commitIndex,
						lastApplied : _lastApplied,
						nextIndex: _nextIndex, 
						matchedIndex  : _matchedIndex}
    
	var command [1] byte 
    copy(command[:], "A")
//  fmt.Print("\nCOMMAND : ",command,len(command),"\n")
    event = Append{data:command}		
    sm.ProcessEvent(event)
	response = GetResponseEventFromChannel()   			

	fmt.Print("\n#",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\n#",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\n#",response,"#\n")
	response = GetResponseEventFromChannel()   			
	fmt.Print("\n#",response,"#\n")


	event = AppendEntriesRespEv{term:1 , success:true}
	sm.ProcessEvent(event)
	fmt.Print("\n#",sm.voteCount,"#\n")	

	event = AppendEntriesRespEv{term:1 , success:true}
	sm.ProcessEvent(event)
	fmt.Print("\n#",sm.voteCount,"#\n")	

	event = AppendEntriesRespEv{term:1 , success:true}
	sm.ProcessEvent(event)
	fmt.Print("\n#",sm.voteCount,"#\n")	

	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES#",response,"#\n")

	event = VoteReqEv{term:1,candidateId:2,lastLogIndex:0,lastLogTerm:0} // out-of-date  request
	sm.ProcessEvent(event)
	fmt.Print("\n#@",sm.state,"#\n")		
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")

	fmt.Print("\n#TERM1@",sm.currentTerm,"#\n")		
	event  = VoteRespEv{term:3 ,voteGranted:false}
	sm.ProcessEvent(event)
	fmt.Print("\n#TERM2@",sm.currentTerm,"#\n")		

	event  = VoteRespEv{term:4 ,voteGranted:true}
	sm.ProcessEvent(event)
	fmt.Print("\n#TERM3@",sm.currentTerm,"#\n")		
		


 	event = VoteReqEv{term:1,candidateId:2,lastLogIndex:1,lastLogTerm:0} // out-of-date  request
	sm.ProcessEvent(event)
	fmt.Print("\n#@",sm.state,"#\n")		
	response = GetResponseEventFromChannel()   			
	fmt.Print("\nRES@",response,"#\n")
	//2fmt.Print("\nRES@",sm.votedFor,"#\n")

	event = Timeout{}
	sm.ProcessEvent(event)




}



// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a == b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}


