package mapreduce

import (
	"bufio"
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
	"strings"
	"time"
)

func collectQueryResult(resultTable map[int64][]JobInfo, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, originRequest MapReduceMsg, jobID int) {
	// 1. merge the result into one file
	resultName := fmt.Sprintf("sql_result_%d", jobID)
	resultFile, _ := os.Create(resultName)
	for nodeID, joblist := range resultTable {
		if nodeID == consts.LEADER_ID {
			continue
		}
		for _, job := range joblist {
			file, err := os.Open(job.fileName)
			if err != nil {
				fmt.Println("Error opening file:", err)
				continue
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				line := scanner.Text()
				resultFile.WriteString(line + "\n")
			}
		}
	}
	resultFile.Close()
	// 2. put the result to SDFS
	sdfs.LeaderPut(resultName, resultName, fileTable, 0, membershipMap)
	// 3. send ack to query function initiator
	initiatorID := originRequest.ID
	startTime := originRequest.Time
	msg := MapReduceMsg{
		JobID:    jobID,
		MsgType:  QUERY_FINISH,
		FileName: resultName,
		ID:       LEADER_ID,
		Duration: time.Since(startTime),
	}
	fmt.Printf("query takes %v\n", msg.Duration)
	destAddr := strings.Split((*membershipMap)[initiatorID].Addr, ":")[0] + ":" + consts.MR_PORT
	conn, _ := net.Dial("tcp", destAddr)
	err := sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending query finish message:", err)
		return
	}
}
