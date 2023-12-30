package mapreduce

import (
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"sync"
)

// HandlerMapReduceLeader handles the map reduce request from worker nodes
func HandlerMapReduceLeader(myHostName string, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, downChannel chan int64) {
	myAddr := myHostName + ":" + consts.MR_PORT
	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()
	// map job id to map (map node id to file name(node id 0 is the prefix file))
	jobMaps := make(map[int]map[int64][]JobInfo)
	requestRecord := make([]MapReduceMsg, 0)
	var mu sync.Mutex
	jobID := 1
	go checkJobStatus(&requestRecord, jobMaps, &mu, fileTable, membershipMap, downChannel)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting:", err)
			return
		}
		request, err := receiveMapReduceRequestJSON(conn)
		if err != nil {
			continue
		}
		// map message
		if request.MsgType == MAP_REQUEST {
			fmt.Printf("Received map request from %d\n", request.ID)
			jobMaps[jobID] = make(map[int64][]JobInfo)
			go handleMapQueryRequestLeader(conn, request, fileTable, membershipMap, &mu, jobMaps[jobID], jobID, &requestRecord)
			jobID += 1

		} else if request.MsgType == MAP_RESULT {
			fmt.Printf("Received map result from %d\n", request.ID)
			go handleResultLeader(request, conn, fileTable, membershipMap, &mu, jobMaps[request.JobID], &requestRecord)
			// reduce message
		} else if request.MsgType == REDUCE_REQUEST {
			fmt.Printf("Received reduce request from %d\n", request.ID)
			jobMaps[jobID] = make(map[int64][]JobInfo)
			go handleReduceRequestLeader(request, fileTable, membershipMap, &mu, jobMaps[jobID], jobID, &requestRecord)
			jobID += 1

		} else if request.MsgType == REDUCE_RESULT {
			fmt.Printf("Received reduce result from %d\n", request.ID)
			go handleReduceResultLeader(request, conn, fileTable, membershipMap, jobMaps[request.JobID], jobID, &requestRecord)
			// query message
		} else if request.MsgType == QUERY_REQUEST {
			fmt.Printf("Received query request from %d\n", request.ID)
			jobMaps[jobID] = make(map[int64][]JobInfo)
			go handleMapQueryRequestLeader(conn, request, fileTable, membershipMap, &mu, jobMaps[jobID], jobID, &requestRecord)
			jobID += 1

		} else if request.MsgType == QUERY_RESULT {
			fmt.Printf("Received query result from %d\n", request.ID)
			go handleResultLeader(request, conn, fileTable, membershipMap, &mu, jobMaps[request.JobID], &requestRecord)
			// join message
		} else if request.MsgType == JOIN_REQUEST {
			fmt.Printf("Received join request from %d\n", request.ID)
			go handleJoinLeader(request, fileTable, membershipMap, &mu, jobMaps[jobID], jobID, &requestRecord)
			jobID += 1

		} else {
			fmt.Println("Map Reduce message not recognized, something wrong happened")
			fmt.Printf("%v\n", request)
			fmt.Println("------------------------------------------------------------")
		}
	}
}
