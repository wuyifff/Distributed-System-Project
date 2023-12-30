package mapreduce

import (
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"sync"
)

// HandlerMapReduceWorker handles the map reduce request from leader, it's sequential
func HandlerMapReduceWorker(myHostName string, fileTable *sdfs.FileTable, mu *sync.Mutex, membershipMap *gossip.MembershipMap, myID int64, jobFinishchan chan int) {
	myAddr := myHostName + ":" + consts.MR_PORT
	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()
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
		if request.MsgType == MAP_START && request.ID == consts.LEADER_ID {
			fmt.Printf("Received map request from leader\n")
			go handleMapRequestWorker(request, conn, fileTable, membershipMap, myID)
		} else if request.MsgType == MAP_FINISH && request.ID == consts.LEADER_ID {
			fmt.Printf("Map jobID:%d finished, file name: %s\n", request.JobID, request.FileName)
			go handleMapResultWorker(request, myID, fileTable, membershipMap, jobFinishchan)

			// reduce message
		} else if request.MsgType == REDUCE_START && request.ID == consts.LEADER_ID {
			fmt.Printf("Received reduce request from leader\n")
			go handleReduceRequestWorker(request, conn, fileTable, membershipMap, myID)
		} else if request.MsgType == REDUCE_FINISH && request.ID == consts.LEADER_ID {
			fmt.Printf("Reduce jobID:%d finished, file name: %s\n", request.JobID, request.FileName)
			go handleReduceResultWorker(request, myID, fileTable, membershipMap, jobFinishchan)

			// query message
		} else if request.MsgType == QUERY_START && request.ID == consts.LEADER_ID {
			fmt.Printf("Received query request from leader\n")
			go handleQueryRequestWorker(request, conn, fileTable, membershipMap, myID)
		} else if request.MsgType == QUERY_FINISH && request.ID == consts.LEADER_ID {
			fmt.Printf("Query jobID:%d finished, file name: %s\n", request.JobID, request.FileName)
			go handleQueryResultWorker(request, myID, fileTable, membershipMap, jobFinishchan)

			// join message
		} else if request.MsgType == JOIN_FINISH && request.ID == consts.LEADER_ID {
			fmt.Printf("Received join request from leader\n")
			go handleJoinResultWorker(request, myID, fileTable, membershipMap, jobFinishchan)
		} else {
			fmt.Println("Map Reduce message not recognized, something wrong happened")
			fmt.Printf("%v\n", request)
		}
	}
}
