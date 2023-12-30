package mapreduce

import (
	"fmt"
	"io"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
)

func sendJob(curID int64, originID int64, jobID int, request MapReduceMsg, membershipMap *gossip.MembershipMap, fileTable *sdfs.FileTable) int64 {
	var msg MapReduceMsg
	var executable string
	var src string
	var dst string
	if request.MsgType == QUERY_REQUEST {
		executable = "grep"
		src = request.Dataset[0]
		dst = fmt.Sprintf("sql_result_%d", jobID)
	} else if request.MsgType == MAP_REQUEST {
		executable = request.Executable
		src = request.SrcDir
		dst = request.Prefix
	}
	num_nodes := request.NumNodes
	// 4.1 notify the node to start map/sql
	if request.MsgType == QUERY_REQUEST {
		msg = MapReduceMsg{
			MsgType:    QUERY_START,
			Executable: executable,
			NumNodes:   num_nodes,
			ID:         consts.LEADER_ID,
			JobID:      jobID,
			FileName:   fmt.Sprintf("sql_%d_%d", jobID, originID),
			Query:      request.Query,
			Dataset:    request.Dataset,
		}
	} else if request.MsgType == MAP_REQUEST {
		msg = MapReduceMsg{
			MsgType:    MAP_START,
			Executable: executable,
			NumNodes:   num_nodes,
			Prefix:     dst,
			SrcDir:     src,
			ID:         consts.LEADER_ID,
			JobID:      jobID,
			FileName:   fmt.Sprintf("map_%d_%d", jobID, originID),
			Extra:      request.Extra,
		}
	}
	destAddr := membershipMap.GetHostName(curID) + ":" + consts.MR_PORT
	conn, _ := net.Dial("tcp", destAddr)
	err := sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending start message to worker: ", curID, ": ", err)
		return originID
	}
	// 4.2 get the confirmation from the node
	var reply MapReduceMsg
	reply, err = receiveMapReduceRequestJSON(conn)
	if err != nil {
		fmt.Println("Error reading confirmation response from worker:", err)
		return originID
	}
	if reply.MsgType != MAP_ACK && reply.MsgType != QUERY_ACK {
		fmt.Println("ACK message not recognized, something wrong happened")
		fmt.Printf("%v\n", reply)
		return originID
	} else {
		// fmt.Printf("receive ack from node %d\n", substutionID)
	}

	// 4.3 send the file to the node
	fileToSend, err := os.Open(fmt.Sprintf("split_%d_%d", jobID, originID))
	defer fileToSend.Close()
	fileToSend.Seek(0, 0)
	if err != nil {
		fmt.Println("Error opening temporary file to send:", err)
		return originID
	}
	_, err = io.Copy(conn, fileToSend)
	if err != nil {
		fmt.Println("Error copy temp file to connection:", err)
		return originID
	}
	fmt.Printf("send splited file split_%d_%d to node %d\n", jobID, originID, curID)
	err = conn.Close()
	if err != nil {
		fmt.Println("Error closing sending file connection:", err)
	}
	return 0
}
