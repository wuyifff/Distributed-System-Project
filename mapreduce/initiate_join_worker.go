package mapreduce

import (
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"time"
)

func InitiateJoin(dataset1 string, dataset2 string, col1 string, col2 string, fileTable *sdfs.FileTable, myID int64, membershipMap *gossip.MembershipMap) {
	// 1. check if dataset 1 2 exists
	exist := sdfs.IsFileExist(fileTable, dataset1)
	if !exist {
		fmt.Printf("%s does not exist", dataset1)
		return
	}
	exist = sdfs.IsFileExist(fileTable, dataset2)
	if !exist {
		fmt.Printf("%s does not exist", dataset2)
		return
	}

	// 2. send request to leader
	fmt.Printf("Join request sent to leader\n")
	leaderAddr := consts.LEADER_HOSTNAME + ":" + consts.MR_PORT
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		fmt.Println("Error dialing TCP in maple:", err)
		return
	}
	defer conn.Close()
	msg := MapReduceMsg{
		MsgType:  JOIN_REQUEST,
		NumNodes: 3,
		ID:       myID,
		Dataset:  []string{dataset1, dataset2},
		Fileds:   []string{col1, col2},
		Time:     time.Now(),
	}
	err = sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error send join request to leader:", err)
		return
	}
}
