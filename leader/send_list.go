package main

import (
	"encoding/json"
	"log"
	"main/consts"
	"main/gossip"
	"net"
	"strings"
)

func sendNewList(sdfsFileName string, list []int64, membershipMap *gossip.MembershipMap) {
	msg := consts.ListUpdate{
		FileID:      sdfsFileName,
		ReplicaList: list,
	}
	marshal, err := json.Marshal(msg)
	if err != nil {
		log.Println("error marshal sendNewList ", err)
		return
	}
	for _, member := range *membershipMap {
		memberHostName := strings.Split(member.Addr, ":")[0]
		memberUDPAddr := memberHostName + ":" + consts.LIST_UPDATE_PORT
		// fmt.Printf("sendNewList to %s\n", memberUDPAddr)
		conn, err := net.Dial("udp", memberUDPAddr)
		if err != nil {
			log.Println("error dialing udp sendNewList ", err)
			return
		}
		_, err = conn.Write(marshal)
		if err != nil {
			log.Println("error sending udp sendNewList ", err)
			return
		}

	}
	// log.Println("sendNewList to all, file name and nodes", sdfsFileName, list)
	// fmt.Println("------------------------------------------------------------")
}
