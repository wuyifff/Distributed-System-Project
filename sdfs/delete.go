package sdfs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"main/consts"
	"main/gossip"
	"main/lock"
	"net"
	"os"
)

func DeleteFile(localFileTable *LocalFileTable, sdfsFileName string, fileTable *FileTable, myID int64, membershipMap *gossip.MembershipMap) bool {

	_, exist := (*fileTable)[sdfsFileName]
	if !exist {
		fmt.Println("File not exist in the system, file name:", sdfsFileName)
		return false
	}

	// 1. get the mutual exclusion lock
	res := lock.GetWriteLock(myID)
	if res {
		fmt.Println("Got the write lock")
		defer func() {
			// release the mutual exclusion lock
			conn, _ := net.Dial("tcp", consts.LEADER_ADDR)
			lock.WriteUNLock(conn, myID)
			fmt.Println("Released the write lock")
			fmt.Println("------------------------------------------------------------")
		}()
	} else {
		fmt.Println("Failed to get the write lock")
		return false
	}

	// 2. delete file
	for _, node := range (*fileTable)[sdfsFileName].Replica {
		if node == myID {
			err := os.Remove((*localFileTable)[sdfsFileName])
			if err != nil {
				fmt.Println("Error deleting local file:", err)
				fmt.Println("sdfs filename: ", sdfsFileName, " , local file name: ", (*localFileTable)[sdfsFileName])
				return false
			}
			delete(*localFileTable, sdfsFileName)
		} else {
			member, exist := (*membershipMap)[node]
			if member.Status != "FAILED" && exist {
				TCPConn, err := net.Dial("tcp", member.Addr)
				if err != nil {
					fmt.Println("Error deleting remote file:", err)
					return false
				}
				TCPConn.Write([]byte("DEL\n"))
				TCPConn.Write([]byte(sdfsFileName + "\n"))
				reader := bufio.NewReader(TCPConn)
				readString, err := reader.ReadString('\n')
				readString = readString[:len(readString)-1]
				if err != nil {
					fmt.Println("Error reading delete response:", err)
					return false
				}
				if readString != "OK" {
					fmt.Println("Error reading delete response, got wrong resp", readString)
					return false
				}
			}

		}
	}

	// ask leader to update file list
	UDPConn, err := net.Dial("udp", consts.LEADER_MSG_ADDR)
	if err != nil {
		fmt.Println("Error dialing to leader for delete", err)
		return false
	}
	msg := consts.LeaderMsg{
		MsgType: consts.LEADER_MSG_DEL,
		FileID:  sdfsFileName,
	}
	marshal, _ := json.Marshal(msg)
	_, err = UDPConn.Write(marshal)
	if err != nil {
		fmt.Println("Error writing deleteMSG to leader", err)
		return false
	}

	return true
}

func handleDELETERequest(netReadWriter *bufio.ReadWriter, myID int64, localFileTable *LocalFileTable) {
	sdfsFileName, err := netReadWriter.ReadString('\n')
	sdfsFileName = sdfsFileName[:len(sdfsFileName)-1]
	if err != nil {
		fmt.Println("Error reading sdfsFileName in handleDELETERequest:", err)
		netReadWriter.WriteString("Error reading sdfsFileName in handleDELETERequest:\n")
		netReadWriter.Flush()
		return
	}
	localName := (*localFileTable)[sdfsFileName]
	err = os.Remove(localName)

	if err != nil {
		fmt.Println("Error deleting file:", err)
		fmt.Println("sdfs filename: ", sdfsFileName, " , local file name: ", localName)
		netReadWriter.WriteString("Error deleting file\n")
		netReadWriter.Flush()
		return
	}

	delete(*localFileTable, sdfsFileName)
	netReadWriter.WriteString("OK\n")
	netReadWriter.Flush()
	return

}
