Ratheesh k v
153050057
M-tech 1 CSE
--------------------------------------------------------------------------------
In the three sample testcases
        candidateTestcases(t) : started with candidate state
        leaderTestcases(t) : started with leader state
        followerTestcases(t) : started with follower state
        
Upon later events, there will be changes in the original state
The test cases cover the whose statement of statemachine (coverage 100%)

In testing i am taking 5 servers, with ID 1,2,3,4,5 

        i.e Majority of server means 3 servers
        
leaderId = 0 // there is no knowledge about leader for a particular statemachine

The "Append" request always contain one new entry for the leader's log file.

Three channels are used

        eventChannel   // all append request/response, timeout, vote request/reply pushed here
        actionChannel  // ouput actions of server machines pushed here
        clientChannel  // append data event from client, commit event from leader to client pushed here


Events in eventchannel
------------------------

1. Append(): 
        This is a request from client to append the data to the replicated log. 
2. Timeout:
        A timeout event meaning depend on the state. 
        If the state machine is a leader, it is interpreted as a heartbeat
        If it is a follower or candidate, it is interpreted as an election timeout.
3. AppendEntriesReq: 
        Request message from a Raft state machine, for appending data to the logs
4. AppendEntriesResp: 
        Response from a Raft state machine in response to a AppendEntriesReq event.
5. VoteReq: 
        Vote request from a Raft state machine to become leader.
6. VoteResp: 
        Response to a Vote request.
7. Send(Id, event):
        Send this event to a server node with id 'Id'. The event is one of AppendEntriesReq/Resp or VoteReq/Resp.

Action in actiochannel
--------------

1. Commit(index, data, err):
         Deliver this commit event to client indicate response to previous Append event
2. Alarm(t): 
        Create a Timeout after t milliseconds




















