package gossip

import (
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"time"
)

func SendMembershipList(membershipMap *MembershipMap, conn *net.UDPConn, dropRate int) {
	// Send UDP message
	defer conn.Close()

	// Simulate package loss
	if dropRate != 0 {
		r := rand.New(rand.NewSource(time.Now().UnixMilli()))
		if r.Intn(100) < dropRate {
			log.Println("Gossip message dropped, current drop rate: ", dropRate)
			return
		}
	}

	message, err := json.Marshal(membershipMap)
	if err != nil {
		log.Println("Error encoding JSON:", err.Error())
		return
	}
	_, err = conn.Write(message)
	if err != nil {
		log.Println("Error sending UDP message:", err.Error())
		return
	}
}

func SendMembershipListAddr(membershipMap *MembershipMap, addr string, dropRate int) {
	// Send UDP message
	udpAddress, _ := net.ResolveUDPAddr("udp", addr)
	conn, err := net.DialUDP("udp", nil, udpAddress)
	if err != nil {
		log.Println("Error sending heartbeat:", err)
		return
	}
	SendMembershipList(membershipMap, conn, dropRate)
}
