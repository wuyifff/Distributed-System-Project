package sdfs

import (
	"encoding/json"
	"fmt"
	"main/consts"
	"main/gossip"
	"net"
	"strconv"
	"strings"
)

func Multiread(nodeList []string, sdfsFileName string, fileTable *FileTable, membershipMap *gossip.MembershipMap) {
	// 1. check if file exists
	_, exists := (*fileTable)[sdfsFileName]
	if !exists {
		fmt.Printf("File %s does not exist in SDFS\n", sdfsFileName)
		return
	}
	// 2. send request to every node in the list
	for _, node := range nodeList {
		n, _ := strconv.ParseInt(node, 10, 64)
		nodeAddr := (*membershipMap)[n].Addr
		nodeIP := strings.Split(nodeAddr, ":")[0]
		DestAddr := fmt.Sprintf("%s:%s", nodeIP, consts.GET_MSG_PORT)
		conn, err := net.Dial("udp", DestAddr)
		if err != nil {
			fmt.Println("Error dialing leader when multiread:", err)
			continue
		}
		msg := consts.LeaderMsg{
			MsgType: consts.LEADER_MSG_MULTIREAD,
			FileID:  sdfsFileName,
		}
		marshal, _ := json.Marshal(msg)
		_, err = conn.Write(marshal)
		if err != nil {
			fmt.Println("Error sending update file list message to leader:", err)
			return
		}
		conn.Close()
	}
}
