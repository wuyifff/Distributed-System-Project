package mapreduce

import (
	"fmt"
	"log"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"regexp"
	"time"
)

func InitiateQuery(dataset string, query string, fileTable *sdfs.FileTable, myID int64, membershipMap *gossip.MembershipMap) {
	// 1. check if dataset exists
	_, exist := (*fileTable)[dataset]
	if !exist {
		log.Println("Dataset does not exist in the SDFS!")
		return
	}
	// 2. check if the query is valid regex expression
	_, err := regexp.Compile(query)
	if err != nil {
		fmt.Println("Regex expression invalid: ", err)
		return
	}
	// 3. send request to leader
	leaderAddr := consts.LEADER_HOSTNAME + ":" + consts.MR_PORT
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		fmt.Println("Error dialing TCP in query:", err)
		return
	}
	defer conn.Close()
	msg := MapReduceMsg{
		MsgType:    QUERY_REQUEST,
		Dataset:    []string{dataset},
		Query:      query,
		ID:         myID,
		NumNodes:   5,
		Time:       time.Now(),
		Executable: "grep",
	}
	err = sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending query request to leader:", err)
		return
	}
	fmt.Printf("Query request sent to leader\n")
}
