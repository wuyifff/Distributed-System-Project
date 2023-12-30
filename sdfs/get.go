package sdfs

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"main/consts"
	"main/gossip"
	"main/lock"
	"math/rand"
	"net"
	"os"
)

func Get(localFileName string, sdfsFileName string, fileTable *FileTable, myID int64, membershipMap *gossip.MembershipMap) bool {
	// 1. check if file exists
	value, exists := (*fileTable)[sdfsFileName]
	if !exists {
		fmt.Printf("File %s does not exist in SDFS\n", sdfsFileName)
		return false
	}

	// 2. check if file exists in local
	replica := value.Replica
	alive_replica := make([]int64, 0)
	for _, nodeId := range replica {
		// if nodeId == myID {
		// 	fmt.Printf("File %s already exists in local\n", sdfsFileName)
		// }
		if _, exists := (*membershipMap)[nodeId]; exists {
			alive_replica = append(alive_replica, nodeId)
			log.Printf("File %s has alive replica in node %d\n", sdfsFileName, nodeId)
		}
	}
	if len(alive_replica) == 0 {
		fmt.Printf("File %s does not have alive replica\n", sdfsFileName)
		return false
	}

	// 3. get the mutual exclusion lock
	res := lock.GetReadLock(myID)
	if res {
		fmt.Println("Got the read lock")
		defer func() {
			conn, _ := net.Dial("tcp", consts.LEADER_ADDR)
			lock.ReadUNLock(conn, myID)
			fmt.Println("Released the read lock")
			// fmt.Printf("File %s is successfully downloaded as %s\n", sdfsFileName, localFileName)
			fmt.Println("------------------------------------------------------------")
		}()
	} else {
		fmt.Println("Cannot get read lock")
		return false
	}

	// 4. get file from replica nodes
	destFile, err := os.Create(localFileName)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return false
	}
	defer destFile.Close()

	randomInt64 := rand.Int() % len(alive_replica)
	nodeId := alive_replica[randomInt64]
	fmt.Printf("Randomly choose node %d to get file %s\n", nodeId, sdfsFileName)
	destAddr := (*membershipMap)[nodeId].Addr
	conn, err := net.Dial("tcp", destAddr)
	if err != nil {
		fmt.Println("Error dialing:", err)
		return false
	}
	defer conn.Close()
	netWriter := bufio.NewWriter(conn)
	netReader := bufio.NewReader(conn)
	netReadWriter := bufio.NewReadWriter(netReader, netWriter)
	_, err = netReadWriter.WriteString("GET\n")
	netReadWriter.Flush()
	_, err = netReadWriter.WriteString(sdfsFileName + "\n")
	netReadWriter.Flush()
	// fmt.Printf("start transfer file %s from %s\n", sdfsFileName, destAddr)
	_, err = io.Copy(destFile, netReadWriter)
	if err != nil {
		fmt.Println("Error during copy:", err)
		return false
	}
	// 5. release the lock

	return true
}

func handleGETRequest(netReadWriter *bufio.ReadWriter, myID int64, conn net.Conn) {
	sdfsFileName, err := netReadWriter.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading:", err)
		return
	}

	sdfsFileName = sdfsFileName[:len(sdfsFileName)-1]
	dirPath := fmt.Sprintf("%s/%d/", consts.FILE_STORE_PATH, myID)
	realFileName := SDFSFileNameToLocalFileName(sdfsFileName)
	file, err := os.Open(dirPath + realFileName)
	file.Seek(0, 0)
	_, err = io.Copy(netReadWriter, file)
	if err != nil {
		fmt.Println("Error during copy:", err)
		return
	}
	netReadWriter.Flush()
	fmt.Printf("Handle GET file %s, file send successfully.\n", sdfsFileName)
	conn.Close()
}
