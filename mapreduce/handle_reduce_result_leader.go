package mapreduce

import (
	"fmt"
	"io"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
)

// HandlerMapReduceLeader handles the map reduce result received from worker nodes, and collect the result when everyone is done
func handleReduceResultLeader(request MapReduceMsg, conn net.Conn, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, jobMap map[int64][]JobInfo, jobId int, requestRecord *[]MapReduceMsg) {
	// 5. leader wait for and receives the result
	resultFileName := request.FileName
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
	fmt.Printf("receive Intermediate file %s from node %d\n", resultFileName, nodeID)
	jobMap[nodeID][0].fileName = resultFileName
	for id, jobs := range jobMap {
		if id == 0 {
			continue
		}
		if jobs[0].fileName == "" {
			fmt.Printf("Reduce job %d not finished yet", jobId)
			return
		}
	}
	// 6. barrier achieved, leader collect the result
	fmt.Printf("Reduce job %d finished, start collect the result\n", jobId)
	fmt.Println("------------------------------------------------------------")
	jobID := request.JobID
	originRequest := (*requestRecord)[jobID-1]
	collectReduceResult(jobMap, fileTable, membershipMap, originRequest, jobID)
}
