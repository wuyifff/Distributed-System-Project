package mapreduce

import (
	"bytes"
	"fmt"
	"io"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
	"os/exec"
	"time"
)

func handleMapRequestWorker(request MapReduceMsg, conn net.Conn, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, myID int64) {
	// 1. worker sends ack to leader
	maple_exe := request.Executable
	// num_maples := request.NumNodes
	sdfs_intermediate_filename_prefix := request.Prefix
	sdfs_src_directory := request.SrcDir
	fmt.Printf("receive request %s %s %s\n", maple_exe, sdfs_intermediate_filename_prefix, sdfs_src_directory)
	msg := MapReduceMsg{MsgType: MAP_ACK, JobID: request.JobID}
	err := sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending ack to map Request:", err)
		return
	}
	fmt.Printf("send maple ack to leader\n")
	// 2. worker receives the file
	filename := request.FileName
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error create file:", err)
		return
	}
	// copy file from connection to local file
	_, err = io.Copy(file, conn)
	if err != nil {
		fmt.Println("Error copy file from connection:", err)
		return
	}
	file.Close()
	fmt.Printf("receive splited file %s from leader\n", filename)
	err = conn.Close()
	if err != nil {
		fmt.Println("Error closing connection:", err)
		return
	}

	// 3. worker execute the maple_exe
	result_file := filename + ".out"
	argv := []string{filename, result_file, request.Extra}
	executable := "./" + maple_exe
	cmd := exec.Command(executable, argv...)
	fmt.Printf("execute %s %v\n", executable, argv)
	cmd.Run()

	// 4. worker send the file back to leader
	newconn, err := net.DialTimeout("tcp", consts.LEADER_HOSTNAME+":"+consts.MR_PORT, 1*time.Second)
	if err != nil {
		fmt.Println("Error dialing TCP in map:", err)
		return
	}
	fmt.Printf("generate map result msg with result file %s\n", result_file)
	msg = MapReduceMsg{
		MsgType:  MAP_RESULT,
		ID:       myID,
		FileName: filename,
		JobID:    request.JobID,
	}
	err = sendRequestJSON(newconn, msg)
	if err != nil {
		fmt.Println("Error sending map result to leader:", err)
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
	err = newconn.Close()
	if err != nil {
		fmt.Println("Error closing connection:", err)
		return
	}
	fileToSend.Close()
	fmt.Printf("send Intermediate file to %s to leader\n", filename)
	fmt.Println("------------------------------------------------------------")
}

func handleQueryRequestWorker(request MapReduceMsg, conn net.Conn, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, myID int64) {
	// 1. worker sends ack to leader
	dataSet := request.Dataset[0]
	query := request.Query
	msg := MapReduceMsg{MsgType: QUERY_ACK, ID: myID}
	err := sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending query ack to leader:", err)
		return
	}

	// 2. worker receives the file
	filename := request.FileName
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("Error create file:", err)
		return
	}
	// copy file from connection to local file
	_, err = io.Copy(file, conn)
	if err != nil {
		fmt.Println("Error copy file from connection:", err)
		return
	}
	file.Close()
	fmt.Printf("receive splited file %s from leader\n", filename)
	err = conn.Close()
	if err != nil {
		fmt.Println("Error closing connection:", err)
		return
	}
	// 3. worker execute query
	searchString := []string{"-E", query}
	searchString = append(searchString, filename)
	cmd := exec.Command("grep", searchString...)
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	cmd.Run()

	//4. write the result to local tmp file
	result_file := filename + ".out"
	stdoutStr := stdoutBuf.String()
	outputFile, _ := os.Create(result_file)
	outputFile.WriteString(stdoutStr)
	outputFile.Close()
	fmt.Printf("execute query %s on part of dataSet %s, result is in %s\n", query, dataSet, result_file)

	// 4. worker send the file back to leader
	newconn, err := net.Dial("tcp", consts.LEADER_HOSTNAME+":"+consts.MR_PORT)
	if err != nil {
		fmt.Println("Error dialing leader to send query result:", err)
		return
	}
	msg = MapReduceMsg{
		MsgType:  QUERY_RESULT,
		ID:       myID,
		FileName: filename,
		JobID:    request.JobID,
	}
	err = sendRequestJSON(newconn, msg)
	if err != nil {
		fmt.Println("Error sending query result to leader:", err)
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
	err = newconn.Close()
	if err != nil {
		fmt.Println("Error closing connection:", err)
		return
	}
	fileToSend.Close()
	fmt.Printf("send Intermediate sql file %s to leader\n", filename)
	fmt.Println("------------------------------------------------------------")
}
