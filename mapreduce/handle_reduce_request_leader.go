package mapreduce

import (
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"sort"
	"sync"
)

func handleReduceRequestLeader(request MapReduceMsg, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, mu *sync.Mutex, jobMap map[int64][]JobInfo, jobId int, requestRecord *[]MapReduceMsg) {
	mu.Lock()
	*requestRecord = append(*requestRecord, request)
	mu.Unlock()
	juice_exe := request.Executable
	num_juice := request.NumNodes
	sdfs_intermediate_filename_prefix := request.Prefix
	//sdfs_src_directory := request.SrcDir
	sdfs_dst_name := request.DestFileName

	// 1. generate the map nodes, assign mapJobs
	livingNodes := membershipMap.LivingNodeShuffle()
	nodeMap := make(map[int64]bool)
	for i := 0; i < len(livingNodes) && len(nodeMap) < num_juice; i++ {
		nodeMap[livingNodes[i]] = true
	}
	mu.Lock()
	// add leader job info
	jobMap[0] = append(jobMap[0], JobInfo{
		jobID:      jobId,
		fileName:   sdfs_dst_name,
		initiator:  request.ID,
		originNode: LEADER_ID,
	})
	//  add worker job info
	for key := range nodeMap {
		fmt.Printf("choose reduce node %d\t", key)
		jobMap[key] = append(jobMap[key], JobInfo{
			jobID:      jobId,
			fileName:   "",
			initiator:  request.ID,
			originNode: key,
		})
	}
	mu.Unlock()
	fmt.Println()

	// 2. leader split the work
	fileInDir := sdfs.LsFolder(sdfs_intermediate_filename_prefix, fileTable)
	totalKeys := len(fileInDir)
	// println("number of keys =", len(fileInDir))
	// for _, key := range fileInDir {
	// 	println("key=", key)
	// }
	sort.Strings(fileInDir)
	// println("after sort")
	for _, key := range fileInDir {
		println("key=", key)
	}
	keysForNode := make(map[int64][]string)
	keysEach := totalKeys / num_juice
	current := 0
	var lastNode int64
	for node := range nodeMap {
		var tmpList []string
		for i := 0; i < keysEach && current < totalKeys; i++ {
			tmpList = append(tmpList, fileInDir[current])
			fmt.Printf("assign key %s to node %d\n", fileInDir[current], node)
			current++
		}
		keysForNode[node] = tmpList
		lastNode = node
	}

	for current < totalKeys {
		keysForNode[lastNode] = append(keysForNode[lastNode], fileInDir[current])
		fmt.Printf("assign extra key %s to node %d\n", fileInDir[current], lastNode)
		current++
	}

	// 3. leader notify the node to start
	curNode := 1
	for nodeID := range nodeMap {
		// 4.1 notify the node to start map
		msg := MapReduceMsg{
			MsgType:      REDUCE_START,
			Executable:   juice_exe,
			NumNodes:     num_juice,
			Prefix:       sdfs_intermediate_filename_prefix,
			DestFileName: sdfs_dst_name,
			ID:           consts.LEADER_ID,
			JobID:        jobId,
			KeyList:      keysForNode[nodeID],
			FileName:     fmt.Sprintf("reduce_output_%d_%d", curNode, nodeID),
		}
		destAddr := membershipMap.GetHostName(nodeID) + ":" + consts.MR_PORT
		conn, _ := net.Dial("tcp", destAddr)
		defer conn.Close()
		err := sendRequestJSON(conn, msg)
		if err != nil {
			fmt.Println("Error sending reduce start message to worker ", nodeID, ": ", err)
			return
		}
		// 4.2 get the confirmation from the node
		var reply MapReduceMsg
		reply, err = receiveMapReduceRequestJSON(conn)
		if err != nil {
			fmt.Println("Error reading confirmation response from worker:", err)
			return
		}
		if reply.MsgType != MAP_ACK {
			fmt.Println("Map Reduce message not recognized, something wrong happened")
			fmt.Printf("%v\n", reply)
			continue
		} else {
			// fmt.Printf("receive ack from node %d\n", nodeID)
		}

		curNode += 1
	}
	fmt.Println("------------------------------------------------------------")

}
