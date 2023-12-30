package sdfs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"main/consts"
	"main/gossip"
	"main/lock"
	"net"
	"os"
	"sync"
)

// Put file to SDFS
// first get the write lock, then transfer file to replica nodes, after all modifyt the file table, at last release the lock
func Put(localFileName string, sdfsFileName string, fileTable *FileTable, localFileTable *LocalFileTable, myID int64, membershipMap *gossip.MembershipMap) bool {
	file, err := os.Open(localFileName)
	if err != nil {
		fmt.Println("Error opening uploaded file:", err)
		return false
	}
	defer file.Close()
	// hash := GetHash(file)

	replica := GetReplica(membershipMap, myID)
	isUpdate := false
	// 1. check if it is a update
	list, exists := (*fileTable)[sdfsFileName]
	if exists && len(list.Replica) > 0 {
		fmt.Println("File already exists in SDFS, it's an update")
		isUpdate = true
	}
	// if the file already exists, use the old replica list
	if isUpdate {
		replica = (*fileTable)[sdfsFileName].Replica
	}
	// 2. get the mutual exclusion lock
	res := lock.GetWriteLock(myID)
	if res {
		fmt.Println("Got the write lock")
		defer func() {
			conn, _ := net.Dial("tcp", consts.LEADER_ADDR)
			lock.WriteUNLock(conn, myID)
			fmt.Println("Released the write lock")
			fmt.Println("------------------------------------------------------------")
		}()
	} else {
		fmt.Println("Failed to get the write lock")
		return false
	}
	// 3. store file to local
	dirPath := fmt.Sprintf("%s/%d/", consts.FILE_STORE_PATH, myID)
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		fmt.Println("Error creating directory:", err)
		return false
	}
	fullPath := dirPath + SDFSFileNameToLocalFileName(sdfsFileName)
	destFile, err := os.Create(fullPath)
	if err != nil {
		fmt.Println("Error creating local store file:", err)
		return false
	}
	defer destFile.Close()
	_, err = io.Copy(destFile, file)
	if err != nil {
		fmt.Println("Error during file copy:", err)
		return false
	}
	(*localFileTable)[sdfsFileName] = fullPath
	fmt.Printf("file %s is stored to local path: %s\n", localFileName, fullPath)

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

	// 6. release the mutual exclusion lock
	return true
}

func sendFile(file *os.File, destAddr string, wg *sync.WaitGroup, fileName string) {
	conn, err := net.Dial("tcp", destAddr)
	if err != nil {
		fmt.Println("Error dialing:", err)
		return
	}
	defer conn.Close()

	// send file name
	netWriter := bufio.NewWriter(conn)
	_, err = netWriter.WriteString("PUT\n")
	netWriter.Flush()
	_, err = netWriter.WriteString(fileName + "\n")
	netWriter.Flush()

	// send file content
	file.Seek(0, 0)
	_, err = io.Copy(netWriter, file)
	if err != nil {
		fmt.Println("Error during copy:", err)
		return
	}
	netWriter.Flush()

	// fmt.Printf("File sent to %s successfully.\n", destAddr)
	wg.Done()
}

func handlePUTRequest(netReadWriter *bufio.ReadWriter, myID int64, conn net.Conn, localFileTable *LocalFileTable) {
	sdfsFileName, err := netReadWriter.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	sdfsFileName = sdfsFileName[:len(sdfsFileName)-1]

	dirPath := fmt.Sprintf("%s/%d/", consts.FILE_STORE_PATH, myID)
	err = os.MkdirAll(dirPath, 0755)
	if err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}
	fullPath := dirPath + SDFSFileNameToLocalFileName(sdfsFileName)
	destFile, err := os.Create(fullPath)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, netReadWriter)
	if err != nil {
		fmt.Println("Error during copy:", err)
		return
	}

	// fmt.Printf("File %s received successfully, stored in %s.\n", sdfsFileName, fullPath)
	(*localFileTable)[sdfsFileName] = fullPath

	conn.Close()
}
