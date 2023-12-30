package mapreduce

import (
	"encoding/csv"
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func handleMapQueryRequestLeader(conn net.Conn, request MapReduceMsg, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, mu *sync.Mutex, jobMap map[int64][]JobInfo, jobID int, requestRecord *[]MapReduceMsg) {
	mu.Lock()
	*requestRecord = append(*requestRecord, request)
	mu.Unlock()
	var src string
	var dst string
	if request.MsgType == QUERY_REQUEST {
		src = request.Dataset[0]
		dst = fmt.Sprintf("sql_result_%d", jobID)
	} else if request.MsgType == MAP_REQUEST {
		src = request.SrcDir
		dst = request.Prefix
	}
	num_nodes := request.NumNodes

	// 1,2 generate the map nodes, assign jobs
	livingNodes := membershipMap.LivingNodeShuffle()
	nodeMap := make(map[int64]bool)
	for i := 0; i < len(livingNodes) && len(nodeMap) < num_nodes; i++ {
		nodeMap[livingNodes[i]] = true
	}

	// 3 get and split files, save to local
	err := splitFile(nodeMap, src, fileTable, membershipMap, LEADER_ID, jobID)
	if err != nil {
		fmt.Println("Error splitting file:", err)
		return
	}

	mu.Lock()
	// leader job info
	// query job record the initiator
	if request.MsgType == QUERY_REQUEST {
		fileName := strconv.FormatInt(request.ID, 10)
		jobMap[0] = append(jobMap[0], JobInfo{
			jobID:      jobID,
			fileName:   fileName,
			initiator:  request.ID,
			originNode: LEADER_ID,
		})
		// map job record the prefix
	} else {
		jobMap[0] = append(jobMap[0], JobInfo{
			jobID:      jobID,
			fileName:   dst,
			initiator:  request.ID,
			originNode: LEADER_ID,
		})
	}
	// worker job info
	for key := range nodeMap {
		fmt.Printf("choose map node %d\t", key)
		jobMap[key] = append(jobMap[key], JobInfo{
			jobID:      jobID,
			fileName:   "",
			initiator:  request.ID,
			originNode: key,
		})
	}
	mu.Unlock()
	fmt.Println()

	// 4. leader notify the node and send the file to the map nodes
	// if the job can not be sent, it returns its own id
	// if the job can be sent, it returns 0
	// resendList := make([]int64, 0)
	for nodeID := range nodeMap {
		sendJob(nodeID, nodeID, jobID, request, membershipMap, fileTable)
		// ret := sendJob(nodeID, nodeID, jobID, request, membershipMap, fileTable)
		// if ret != 0 {
		// 	resendList = append(resendList, ret)
		// }
	}
	// time.Sleep((consts.EXPIRE_TIME + consts.DELETE_TIME) * time.Second)
	// for _, nodeID := range resendList {
	// 	mu.Lock()
	// 	subID := membershipMap.LivingNodeShuffle()[0]
	// 	jobMap[subID] = append(jobMap[subID], JobInfo{
	// 		jobID:      jobID,
	// 		fileName:   "",
	// 		initiator:  request.ID,
	// 		originNode: nodeID,
	// 	})
	// 	delete(jobMap, nodeID)
	// 	mu.Unlock()
	// 	sendJob(subID, nodeID, jobID, request, membershipMap, fileTable)

	// }
	fmt.Println("------------------------------------------------------------")
}

func handleJoinLeader(request MapReduceMsg, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, mu *sync.Mutex, jobMap map[int64][]JobInfo, jobID int, requestRecord *[]MapReduceMsg) {
	dataSets := request.Dataset
	cols := request.Fileds
	dataset1 := dataSets[0]
	dataset2 := dataSets[1]
	col1 := cols[0]
	col2 := cols[1]
	// get the datasets
	sdfs.Get(dataset1, dataset1, fileTable, LEADER_ID, membershipMap)
	sdfs.Get(dataset2, dataset2, fileTable, LEADER_ID, membershipMap)

	// read the datasets
	file1, err := os.Open(dataset1)
	if err != nil {
		fmt.Println("Error opening dataset1:", err)
		return
	}
	defer file1.Close()
	file2, err := os.Open(dataset2)
	if err != nil {
		fmt.Println("Error opening dataset2:", err)
		return
	}
	defer file2.Close()
	reader1 := csv.NewReader(file1)
	records1, err := reader1.ReadAll()
	if err != nil {
		fmt.Println("Error reading dataset1:", err)
		return
	}
	reader2 := csv.NewReader(file2)
	records2, err := reader2.ReadAll()
	if err != nil {
		fmt.Println("Error reading dataset2:", err)
		return
	}
	fmt.Printf("%v, %s\n", records1[0], col1)
	index1 := getColumnIndex(records1[0], col1)
	if index1 == -1 {
		fmt.Println("Error: column1 not found")
		return
	}
	fmt.Printf("%v, %s\n", records2[0], col2)
	index2 := getColumnIndex(records2[0], col2)
	if index2 == -1 {
		fmt.Println("Error: column2 not found")
		return
	}

	// use dataset2 build a map
	dataMap := make(map[string][]string)
	for i, record := range records2 {
		if i == 0 {
			continue
		}
		key := record[index2]
		dataMap[key] = record
	}

	outputFile, err := os.Create("output.csv")
	if err != nil {
		fmt.Println("Error creating output file:", err)
		return
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()
	// write header
	header := append(records1[0], records2[0]...)
	err = writer.Write(header)
	if err != nil {
		fmt.Println("Error writing header:", err)
		return
	}
	// write data, use dataset1 to join
	for _, record := range records1 {
		key := record[index1]
		if data, ok := dataMap[key]; ok {
			joinedRecord := append(record, data...)
			err := writer.Write(joinedRecord)
			if err != nil {
				fmt.Println("Error writing joined record:", err)
				return
			}
		}
	}
	fmt.Printf("Join result written to output.csv\n")
	sdfsName := "join_" + strconv.Itoa(jobID) + ".csv"
	sdfs.LeaderPut("output.csv", sdfsName, fileTable, 0, membershipMap)
	// send result to requester
	destAddr := strings.Split((*membershipMap)[request.ID].Addr, ":")[0] + ":" + consts.MR_PORT
	conn, err := net.Dial("tcp", destAddr)
	if err != nil {
		fmt.Println("Error dialing TCP in join result:", err)
		return
	}
	defer conn.Close()
	msg := MapReduceMsg{
		MsgType:  JOIN_FINISH,
		FileName: sdfsName,
		Duration: time.Since(request.Time),
		JobID:    jobID,
	}
	err = sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error send join result:", err)
		return
	}
	fmt.Printf("Join result sent to %d\n", request.ID)

}

func getColumnIndex(header []string, columnName string) int {
	for i, col := range header {
		col = strings.TrimPrefix(col, "\ufeff")
		if col == columnName {
			return i
		}
	}
	return -1
}
