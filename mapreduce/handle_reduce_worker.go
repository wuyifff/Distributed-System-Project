package mapreduce

import (
	"fmt"
	"io"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
	"os/exec"
	"strconv"
)

func handleReduceRequestWorker(request MapReduceMsg, conn net.Conn, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, myID int64) {
	// 1. worker sends ack to leader
	juice_exe := request.Executable
	// num_maples := request.NumNodes
	sdfs_intermediate_filename_prefix := request.Prefix
	assignedKeys := request.KeyList
	jobId := request.JobID
	fmt.Printf("receive request %s %s\n", juice_exe, sdfs_intermediate_filename_prefix)

	// 2. worker gets the file and merge the file
	mergedFileName := "juice_" + strconv.Itoa(jobId) + "_merged"
	mergeFd, err := os.Create(mergedFileName)
	if err != nil {
		fmt.Println("error creating file ", mergedFileName, err)
		return
	}
	for _, key := range assignedKeys {
		sdfs.Get("juice_"+strconv.Itoa(jobId)+"_"+key, key, fileTable, myID, membershipMap)
		fmt.Printf("get key: %s successful, stored as: juice_%d_%s\n", key, jobId, key)
		partialFd, err := os.Open("juice_" + strconv.Itoa(jobId) + "_" + key)
		if err != nil {
			fmt.Println("error creating file juice_"+strconv.Itoa(jobId)+"_merged,", err)
			return
		}
		_, err = io.Copy(mergeFd, partialFd)
		if err != nil {
			fmt.Println("error merging file ", err)
			return
		}
	}

	msg := MapReduceMsg{MsgType: MAP_ACK}
	err = sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending reducd ack result to leader:", err)
		return
	}
	err = conn.Close()
	if err != nil {
		fmt.Println("Error closing connection:", err)
		return
	}

	// 3. worker execute the juice_exe
	result_file := fmt.Sprintf("juice_res_%d_%s.out", jobId, assignedKeys[0])
	argv := []string{mergedFileName, result_file}
	executable := "./" + juice_exe
	cmd := exec.Command(executable, argv...)
	fmt.Printf("execute %s %v\n", executable, argv)
	err = cmd.Run()
	if err != nil {
		fmt.Println("error executing", err)
		return
	}

	// 4. worker send the file back to leader
	newconn, _ := net.Dial("tcp", consts.LEADER_HOSTNAME+":"+consts.MR_PORT)
	defer newconn.Close()
	msg = MapReduceMsg{
		MsgType:  REDUCE_RESULT,
		ID:       myID,
		JobID:    jobId,
		FileName: result_file,
	}
	err = sendRequestJSON(newconn, msg)
	if err != nil {
		fmt.Println("Error sending reduce result to leader:", err)
		return
	}
	fileToSend, err := os.Open(result_file)
	if err != nil {
		fmt.Println("Error opening temporary file to send:", err)
		return
	}

	_, err = io.Copy(newconn, fileToSend)
	if err != nil {
		fmt.Println("Error copy temp file to connection:", err)
		return
	}
	fileToSend.Close()
	fmt.Printf("send Intermediate file to %s to leader\n", result_file)
	fmt.Println("------------------------------------------------------------")
}
