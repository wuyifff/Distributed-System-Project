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

func InitiateMap(maple_exe string, num_maples string, sdfs_intermediate_filename_prefix string, sdfs_src_directory string, interconn string, fileTable *sdfs.FileTable, myID int64, membershipMap *gossip.MembershipMap) {
	// check if maple_exe exists done
	_, err := os.Stat(maple_exe)
	if err != nil {
		fmt.Println("Maple exe does not exist")
		return
	}

	// check if num_maples is valid
	num, err := strconv.Atoi(num_maples)
	if err != nil {
		fmt.Println("Invalid number of maples")
		return
	}
	livingNodes := membershipMap.GetLivingNodeNum()
	if num > livingNodes {
		fmt.Println("Number of maples is greater than number of living nodes")
		return
	}

	// check sdfs_src_directory exists
	exist := sdfs.IsFileExist(fileTable, sdfs_src_directory)
	if !exist {
		maxtries := 5
		t := false
		for i := 0; i < maxtries; i++ {
			exist = sdfs.IsFileExist(fileTable, sdfs_src_directory)
			if exist {
				t = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !t {
			fmt.Printf("%s does not exist", sdfs_src_directory)
			return
		}
	}

	// send request to leader
	fmt.Printf("Maple request sent to leader\n")
	leaderAddr := consts.LEADER_HOSTNAME + ":" + consts.MR_PORT
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		fmt.Println("Error dialing TCP in maple:", err)
		return
	}
	defer conn.Close()
	msg := MapReduceMsg{
		MsgType:    MAP_REQUEST,
		Executable: maple_exe,
		NumNodes:   num,
		Prefix:     sdfs_intermediate_filename_prefix,
		SrcDir:     sdfs_src_directory,
		ID:         myID,
		Time:       time.Now(),
		Extra:      interconn,
	}
	err = sendRequestJSON(conn, msg)
	if err != nil {
		fmt.Println("Error send map request to leader:", err)
		return
	}

	// // wait for ack
	// buf := make([]byte,1024*1024)
	// n, err := conn.Read(buf)
	// if err != nil {
	// 	fmt.Println("Error reading response for leader map:", err)
	// 	return
	// }
	// var response MapReduceMsg
	// err = json.Unmarshal(buf[:n], &response)
	// if err != nil {
	// 	fmt.Println("Error unmarshaling response for leader map:", err)
	// 	return
	// }
	// if response.MsgType == MAP_ACK {
	// 	fmt.Println("Maple request accepted")
	// } else {
	// 	fmt.Println("Maple request rejected")
	// }
}
