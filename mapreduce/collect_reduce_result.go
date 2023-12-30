package mapreduce

import (
	"fmt"
	"io"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
	"sort"
	"strings"
	"time"
)

// collectReduceResult collects the result of reduce workers, per node
func collectReduceResult(resultTable map[int64][]JobInfo, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, orginRequest MapReduceMsg, jobID int) {
	destFileName := resultTable[0][0].fileName

	var files []string
	for k, v := range resultTable {
		if k == 0 {
			continue
		}
		files = append(files, v[0].fileName)
	}
	sort.Strings(files)
	println("files are")
	for _, i := range files {
		println(i)
	}

	// merge partial results from each node
	destFd, err := os.Create(destFileName)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	for _, filename := range files {
		if filename == destFileName {
			continue
		}
		readFd, err := os.Open(filename)
		if err != nil {
			fmt.Println("Error opening file:", err)
			continue
		}
		_, err = io.Copy(destFd, readFd)
		if err != nil {
			fmt.Println("Error merging file:", err)
			return
		}

	}
	fmt.Println("Reduce result collected")

	// leader put the result to SDFS
	sdfs.LeaderPut(destFileName, destFileName, fileTable, 0, membershipMap)

	// send ack to map function initiator
	initiatorID := orginRequest.ID
	msg := MapReduceMsg{
		JobID:    jobID,
		MsgType:  REDUCE_FINISH,
		FileName: destFileName,
		Duration: time.Since(orginRequest.Time),
	}
	destAddr := strings.Split((*membershipMap)[initiatorID].Addr, ":")[0] + ":" + consts.MR_PORT
	conn, _ := net.Dial("tcp", destAddr)
	err = sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending reduce finish message:", err)
		return
	}
}
