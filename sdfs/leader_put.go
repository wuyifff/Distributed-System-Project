package sdfs

import (
	"encoding/json"
	"fmt"
	"main/consts"
	"main/gossip"
	"net"
	"os"
	"sync"
)

// leader Put file to SDFS
func LeaderPut(localFileName string, sdfsFileName string, fileTable *FileTable, myID int64, membershipMap *gossip.MembershipMap) bool {
	file, err := os.Open(localFileName)
	if err != nil {
		fmt.Println("Error opening uploaded file:", err)
		return false
	}
	defer file.Close()
	replica := membershipMap.LivingNodeShuffle()[:3]
	// fmt.Printf("Leader put file %s replica nodes are %v\n", sdfsFileName, replica)
	isUpdate := false
	// 1. check if it is a update
	list, exists := (*fileTable)[sdfsFileName]
	if exists && len(list.Replica) > 0 {
		// fmt.Println("File already exists in SDFS, it's an update")
		isUpdate = true
	}
	// if the file already exists, use the old replica list
	if isUpdate {
		replica = (*fileTable)[sdfsFileName].Replica
	}
	// 2. get the mutual exclusion lock
	// res := lock.GetWriteLock(myID)
	// if res {
	// 	fmt.Println("Got the write lock")
	// 	defer func() {
	// 		conn, _ := net.Dial("tcp", consts.LEADER_ADDR)
	// 		lock.WriteUNLock(conn, myID)
	// 		fmt.Println("Released the write lock")
	// 		fmt.Println("------------------------------------------------------------")
	// 	}()
	// } else {
	// 	fmt.Println("Failed to get the write lock")
	// 	return false
	// }
	// 3. store file to local

	// 4. transfer file to replica nodes
	var wg sync.WaitGroup
	for _, nodeId := range replica {
		if nodeId == -1 || nodeId == myID {
			continue
		}
		// send file to replica node
		destAddr := (*membershipMap)[nodeId].Addr
		wg.Add(1)
		file.Seek(0, 0)
		sendFile(file, destAddr, &wg, sdfsFileName)
	}
	wg.Wait()

	// 5. modify the file table, ask leader to broadcast
	msg := consts.LeaderMsg{
		MsgType: consts.LEADER_MSG_PUT,
		SelfID:  myID,
		FileID:  sdfsFileName,
		MachID:  myID,
		Replica: replica,
	}
	marshal, _ := json.Marshal(msg)
	conn, _ := net.Dial("udp", consts.LEADER_MSG_ADDR)
	defer conn.Close()
	_, err = conn.Write(marshal)
	if err != nil {
		fmt.Println("Error sending update file list message to leader:", err)
		return false
	}
	return true
}
