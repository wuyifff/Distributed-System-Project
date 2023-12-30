package main

import (
	"encoding/json"
	"log"
	"main/consts"
	"main/gossip"
	"main/lock"
	"main/mapreduce"
	"main/sdfs"
	"net"
	"os"
	"sort"
	"strings"
)

func main() {
	args := os.Args
	if len(args) != 2 {
		log.Println("Usage: go run server.go <serverAddr>")
		return
	}
	membershipMap := make(gossip.MembershipMap)
	downVote := make(map[int64]int64)
	myAddr := args[1]
	myHostName := consts.LEADER_HOSTNAME

	fileTable := make(sdfs.FileTable)

	myIP := strings.Split(myAddr, ":")[0]
	msgAddr := myIP + ":" + consts.LEADER_MSG_PORT
	udpMSGAddress, err := net.ResolveUDPAddr("udp", msgAddr)
	MSGConn, err := net.ListenUDP("udp", udpMSGAddress)
	if err != nil {
		log.Println("Error listening MSG:", err)
		return
	}
	defer MSGConn.Close()
	downChannel := make(chan int64)
	log.Printf("Introducer is now listening MSG on %s", msgAddr)
	go receiveUpdateMsg(&fileTable, &downVote, &membershipMap, MSGConn, downChannel)

	udpAddress, err := net.ResolveUDPAddr("udp", myAddr)
	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		log.Println("Error listening:", err)
		return
	}
	defer conn.Close()
	log.Printf("Introducer is now listening gossip on %s", myAddr)
	var count int64 = 1

	buffer := make([]byte, 4096)
	RWlock := lock.NewLock()
	go lock.HandleLockRequest(myAddr, RWlock)
	go mapreduce.HandlerMapReduceLeader(myHostName, &fileTable, &membershipMap, downChannel)

	// Receive UDP message, and make membership list
	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Error reading:", err)
			return
		}
		var newMember gossip.MembershipMap
		json.Unmarshal(buffer[:n], &newMember)

		// Add new member to membership list
		var member gossip.Member

		// there is only one member in newMember
		for _, v := range newMember {
			member = v
		}
		// log.Printf("Get member request: %+v\n", member)

		// check if the new member is already in the membership list and delete it
		var member_tobe_deleted int64 = -1
		for _, membership := range membershipMap {
			if membership.Addr == member.Addr {
				member_tobe_deleted = membership.ID
				break
			}
		}
		if member_tobe_deleted != -1 {
			log.Printf("member id:%d add:%s is deleted\n\n", member_tobe_deleted, member.Addr)
			delete(membershipMap, member_tobe_deleted)
		}

		member.ID = count
		membershipMap[count] = member
		count += 1

		downVote[member.ID] = 0
		// sleep before Send UDP message
		returnAddr, err := net.ResolveUDPAddr("udp", member.Addr)
		conn, err := net.DialUDP("udp", nil, returnAddr)
		gossip.SendMembershipList(&membershipMap, conn, 0)

		// Print membership list
		members := make([]gossip.Member, 0, len(membershipMap))
		for _, member := range membershipMap {
			members = append(members, member)
		}
		// Sort the slice by ID
		sort.Slice(members, func(i, j int) bool {
			return members[i].ID < members[j].ID
		})
		log.Printf("send membershipMap: \n")
		log.Printf("+------+------------------------------------------+------------+------------+\n")
		log.Printf("|  ID  |                   Addr                   | Heartbeat  |   Status   |\n")
		log.Printf("+------+------------------------------------------+------------+------------+\n")

		for _, member := range members {
			log.Printf("| %-4d | %-40s | %-10d | %-10s |\n",
				member.ID, member.Addr, member.Heartbeat, member.Status)
		}
		log.Printf("+------+------------------------------------------+------------+------------+\n\n")
		for fileName, replicaList := range fileTable {
			sendNewList(fileName, replicaList.Replica, &membershipMap)
			log.Println(fileName, " , replica list: ", replicaList.Replica)
		}
	}

}
