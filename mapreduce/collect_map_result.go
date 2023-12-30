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

// collectMapResult collects the result of map workers, per key per file
func collectMapResult(resultTable map[int64][]JobInfo, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, originRequest MapReduceMsg, jobID int) {
	prefix := resultTable[0][0].fileName
	collection := make(map[string]*os.File)
	for _, joblist := range resultTable {
		if joblist[0].fileName == prefix {
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
				key := strings.Split(line, " ")[0]
				key = sdfs.SDFSFileNameToLocalFileName(key)
				if _, exist := collection[key]; !exist {
					collection[key], err = os.Create(prefix + "_" + key)
					if err != nil {
						fmt.Println("Error creating file:", err)
						return
					}
					defer collection[key].Close()
				}
				collection[key].WriteString(line + "\n")
			}
		}
	}
	fmt.Println("Map result collected")

	// leader put the result to SDFS
	for key, file := range collection {
		res := sdfs.LeaderPut(file.Name(), prefix+"_"+key, fileTable, 0, membershipMap)
		if res {
			fmt.Printf("Successfully put %s to SDFS\n", prefix+"_"+key)
		} else {
			fmt.Printf("Failed to put %s to SDFS\n", prefix+"_"+key)
		}
		fmt.Println("------------------------------------------------------------")
	}

	// send ack to map function initiator
	initiator := originRequest.ID
	startTime := originRequest.Time
	msg := MapReduceMsg{
		JobID:    jobID,
		MsgType:  MAP_FINISH,
		FileName: prefix,
		ID:       LEADER_ID,
		Duration: time.Since(startTime),
	}
	fmt.Printf("map takes %v\n", msg.Duration)
	destAddr := strings.Split((*membershipMap)[initiator].Addr, ":")[0] + ":" + consts.MR_PORT
	conn, _ := net.Dial("tcp", destAddr)
	err := sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending map finish message to ", destAddr, ": ", err)
		return
	}
}
