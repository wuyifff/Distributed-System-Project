package mapreduce

import (
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"os"
	"strconv"
	"time"
)

func InitiateReduce(juice_exe string, num_juices string, sdfs_intermediate_filename_prefix string, sdfs_dest_filename string,
	delete_input string, fileTable *sdfs.FileTable, myID int64, membershipMap *gossip.MembershipMap) {
	// check if juice_exe exists
	_, err := os.Stat(juice_exe)
	if err != nil {
		fmt.Println("juice exe does not exist")
		return
	}

	// check if num_juices is valid
	num, err := strconv.Atoi(num_juices)
	if err != nil {
		fmt.Println("Invalid number of juice")
		return
	}
	livingNodes := membershipMap.GetLivingNodeNum()
	if num > livingNodes {
		fmt.Println("Number of juice is greater than number of living nodes")
		return
	}

	// check if map result exists
	exist := sdfs.IsFileExist(fileTable, sdfs_intermediate_filename_prefix)
	if !exist {
		maxtries := 5
		t := false
		for i := 0; i < maxtries; i++ {
			exist = sdfs.IsFileExist(fileTable, sdfs_intermediate_filename_prefix)
			if exist {
				t = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !t {
			fmt.Printf("%s does not exist", sdfs_intermediate_filename_prefix)
			return
		}
	}

	// send request to leader
	fmt.Printf("Juice request sent to leader\n")
	leaderAddr := consts.LEADER_HOSTNAME + ":" + consts.MR_PORT
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		fmt.Println("Error dialing TCP in Juice:", err)
		return
	}
	defer conn.Close()
	msg := MapReduceMsg{
		MsgType:      REDUCE_REQUEST,
		Executable:   juice_exe,
		NumNodes:     num,
		DestFileName: sdfs_dest_filename,
		Prefix:       sdfs_intermediate_filename_prefix,
		ID:           myID,
		Time:         time.Now(),
	}
	err = sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error sending reduce request to leader:", err)
		return
	}

}
