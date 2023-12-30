package main

import (
	"encoding/json"
	"fmt"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
	"strconv"
	"strings"
)

func ListenCommand(myAddr string, myID int64, membershipMap *gossip.MembershipMap, fileTable *sdfs.FileTable, localFileTable *sdfs.LocalFileTable) {
	myIP := strings.Split(myAddr, ":")[0]
	UDPAddr := myIP + ":" + consts.GET_MSG_PORT
	myUDPAddr, err := net.ResolveUDPAddr("udp", UDPAddr)
	UDPListen, err := net.ListenUDP("udp", myUDPAddr)
	if err != nil {
		fmt.Println("Error listening UDP command", err)
		return
	}
	fmt.Println("Listening Command on " + UDPAddr)
	defer UDPListen.Close()
	buffer := make([]byte, 1024*1024)
	for {
		n, remoteAddr, err := UDPListen.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading UDP command:", err)
			return
		}
		msg := consts.LeaderMsg{}
		err = json.Unmarshal(buffer[:n], &msg)
		fmt.Printf("Received command UDP message %v from %s\n", msg, remoteAddr)
		if err != nil {
			fmt.Println("Error Unmarshal UDP command:", err)
			return
		}
		switch msg.MsgType {
		case consts.LEADER_MSG_MULTIREAD:
			sdfs.Get(msg.FileID, msg.FileID, fileTable, myID, membershipMap)

		case consts.LEADER_MSG_REPLICA:
			filePath := consts.FILE_STORE_PATH + "/" + strconv.FormatInt(myID, 10) + "/" + msg.FileID
			sdfs.Get(filePath, msg.FileID, fileTable, myID, membershipMap)
			(*localFileTable)[msg.FileID] = filePath
			resp := consts.LeaderResp{
				RespType: consts.LEADER_RESP_OK,
			}
			marshal, err := json.Marshal(resp)
			if err != nil {
				fmt.Println("Error Unmarshal Replica respond:", err)
				return
			}
			_, err = UDPListen.WriteToUDP(marshal, remoteAddr)
			if err != nil {
				fmt.Println("Error sending Replica respond:", err)
				return
			}
		}
	}
}
