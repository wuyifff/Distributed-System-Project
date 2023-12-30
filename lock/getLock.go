package lock

import (
	"encoding/json"
	"fmt"
	"main/consts"
	"net"
	"time"
)

func GetWriteLock(myID int64) bool {
	leaderAddr := consts.LEADER_ADDR
	count := 0
	maxcount := consts.MAX_TRY_TIMES
	for {
		conn, err := net.Dial("tcp", leaderAddr)
		if err != nil {
			fmt.Printf("Error connecting to leader %s: %s\n", leaderAddr, err)
			return false
		}
		writeRequest := LockRequest{
			MsgType:     WRITE_LOCK,
			RequestorID: myID,
		}
		marshal, _ := json.Marshal(writeRequest)
		_, err = conn.Write(marshal)
		buffer := make([]byte, 1024*1024)
		n, err := conn.Read(buffer)
		var feedback LockResponse
		err = json.Unmarshal(buffer[:n], &feedback)
		if err != nil {
			fmt.Println("Error unmarshall leader message:", err)
			return false
		}
		if feedback.MsgType == OK {
			return true
		} else if feedback.MsgType == WAIT && count < maxcount {
			fmt.Printf("Write lock is not available, waiting... for %dms\n", consts.WAIT_TIME)
			time.Sleep(time.Duration(consts.WAIT_TIME) * time.Millisecond)
			count += 1
		} else if count >= maxcount {
			fmt.Printf("Error getting write lock, tried %d times\n", maxcount)
			return false
		} else {
			fmt.Println("Message not recognized, something wrong happened")
			return false
		}
	}
}

func GetReadLock(myID int64) bool {
	leaderAddr := consts.LEADER_ADDR
	count := 0
	maxcount := 100
	for {
		conn, err := net.Dial("tcp", leaderAddr)
		if err != nil {
			fmt.Printf("Error connecting to leader %s: %s\n", leaderAddr, err)
			return false
		}
		readRequest := LockRequest{
			MsgType:     READ_LOCK,
			RequestorID: myID,
		}
		marshal, _ := json.Marshal(readRequest)
		_, err = conn.Write(marshal)
		buffer := make([]byte, 1024*1024)
		n, err := conn.Read(buffer)
		var feedback LockResponse
		err = json.Unmarshal(buffer[:n], &feedback)
		if feedback.MsgType == OK {
			return true
		} else if feedback.MsgType == WAIT && count < maxcount {
			fmt.Printf("Read lock is not available, waiting... for %dms\n", consts.WAIT_TIME)
			time.Sleep(time.Duration(consts.WAIT_TIME) * time.Millisecond)
			count += 1
		} else if count >= maxcount {
			fmt.Printf("Error getting read lock, tried %d times\n", maxcount)
			return false
		} else {
			fmt.Println("Message not recognized")
			return false
		}
	}
}
