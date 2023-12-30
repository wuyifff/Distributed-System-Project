package mapreduce

import (
	"fmt"
	"io"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

func handleResultLeader(request MapReduceMsg, conn net.Conn, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, mu *sync.Mutex, jobMap map[int64][]JobInfo, requestRecord *[]MapReduceMsg) {
	// 1. parse the request type
	var prefix string
	if request.MsgType == MAP_RESULT {
		prefix = "map"
	} else if request.MsgType == REDUCE_RESULT {
		prefix = "reduce"
	} else if request.MsgType == QUERY_RESULT {
		prefix = "query"
	}
	// 2. store the result file
	resultFileName := fmt.Sprintf("%s_result", request.FileName)
	nodeID := request.ID
	resultFile, err := os.Create(resultFileName)
	if err != nil {
		fmt.Println("Error creating result file:", err)
		return
	}
	defer resultFile.Close()
	_, err = io.Copy(resultFile, conn)
	if err != nil {
		fmt.Println("Error copy result file from connection:", err)
		return
	}
	fmt.Printf("receive Intermediate file %s from node %d, jobID %d, saved as %s\n", request.FileName, nodeID, request.JobID, resultFileName)

	// 3. figure out which node has this job, and who this job belongs to
	originIDstring := strings.Split(request.FileName, "_")[2]
	originID, err := strconv.ParseInt(originIDstring, 10, 64)

	// 4. record the result file name to the jobMap
	match := false
	curJobList := jobMap[nodeID]
	mu.Lock()
	for index := range curJobList {
		if curJobList[index].fileName == "" && curJobList[index].originNode == originID {
			curJobList[index].fileName = resultFileName
			match = true
			break
		}
	}
	if !match {
		fmt.Printf("Error: no match job for %s\n node's job details %v", resultFileName, jobMap[nodeID])
	}
	fmt.Printf("------------------jobMap %d------------------\n", request.JobID)
	for key, value := range jobMap {
		fmt.Printf("assigned nodes: %d, value: %v\n", key, value)
	}
	fmt.Printf("------------------jobMap %d------------------\n", request.JobID)

	// 5. check if all the jobs are finished, if not finished, not collect the result
	finished := true
	for id, nodeJobs := range jobMap {
		if id == 0 {
			continue
		}
		if !finished {
			break
		}
		for _, job := range nodeJobs {
			if job.fileName == "" {
				fmt.Println("Map job not finished yet")
				finished = false
				break
			}
		}
	}
	mu.Unlock()
	if !finished {
		return
	}

	// 6. barrier achieved, leader collect the result
	fmt.Printf("%s job finished, start collect the result\n", prefix)
	fmt.Println("------------------------------------------------------------")
	// collectMapResult(jobMap, fileTable, mu, membershipMap)
	jobID := request.JobID
	originRequest := (*requestRecord)[jobID-1]
	if prefix == "query" {
		collectQueryResult(jobMap, fileTable, membershipMap, originRequest, jobID)
	} else {
		collectMapResult(jobMap, fileTable, membershipMap, originRequest, jobID)
	}
}
