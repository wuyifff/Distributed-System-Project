package main

import (
	"encoding/json"
	"fmt"
	"log"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

func sendNodeFailMSG(myID int64, failedId int64) {
	dial, err := net.Dial("udp", consts.LEADER_MSG_ADDR)
	if err != nil {
		log.Println("Error dialing UDP to leader when sending member failed MSG", err)
		return
	}
	msg := consts.LeaderMsg{
		MsgType: consts.LEADER_MSG_DOWN,
		SelfID:  myID,
		MachID:  failedId,
	}
	marshal, err := json.Marshal(msg)
	if err != nil {
		log.Println("Error marshalling LEADER_MSG_DOWN ", err)
		return
	}
	_, err = dial.Write(marshal)
	if err != nil {
		log.Println("Error sending LEADER_MSG_DOWN ", err)
		return
	}
}

// cleanup delete member from membership list after DELETE_TIME
func cleanup(id int64, mutex *sync.Mutex, membershipMap *gossip.MembershipMap) {
	time.Sleep(consts.DELETE_TIME * time.Second)
	log.Printf("member %d is deleted\n", id)
	fmt.Printf("member %d is deleted\n", id)
	mutex.Lock()
	delete(*membershipMap, id)
	mutex.Unlock()
}

// removeExpired mark failed nodes as FAILED
func removeExpired(membershipMap *gossip.MembershipMap, myID int64, mutex *sync.Mutex, f *os.File) {
	var expireTime time.Duration = consts.EXPIRE_TIME * time.Second

	for i, member := range *membershipMap {
		if time.Since(member.LocalTime) > expireTime &&
			member.ID != myID &&
			member.Status != "FAILED" {

			log.Printf("member %d is failed, last receive time %s, heartbeat %d, now is %v\n", member.ID, member.LocalTime.Format("2006-01-02 15:04:05.000"), member.Heartbeat, time.Now().Format("2006-01-02 15:04:05.000"))
			// fmt.Printf("member %d is failed, last receive time %s, heartbeat %d, now is %v\n", member.ID, member.LocalTime.Format("2006-01-02 15:04:05.000"), member.Heartbeat, time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("member %d is failed\n", member.ID)
			f.WriteString(time.Now().Format("15:04:05.999999") + "\n")
			member.Status = "FAILED"

			mutex.Lock()
			(*membershipMap)[i] = member
			mutex.Unlock()
			sendNodeFailMSG(myID, member.ID)

			go cleanup(member.ID, mutex, membershipMap)
			// gossip.PrintMembershipListOnce(membershipMap)

		}
	}
}

// removeExpiredSus mark suspected nodes as SUSPECTED, and remove SUSPECTED nodes after EXPIRE_TIME
func removeExpiredSus(membershipMap *gossip.MembershipMap, myID int64, mutex *sync.Mutex, f *os.File) {
	var suspectTime time.Duration = consts.SUSPECT_TIME * time.Second
	var expireTime time.Duration = consts.EXPIRE_TIME*time.Second - suspectTime

	mutex.Lock()
	for i, member := range *membershipMap {
		if time.Since(member.LocalTime) > suspectTime &&
			member.ID != myID &&
			member.Status == "ALIVE" {
			log.Printf("member %d is suspected, last receive time %s, heartbeat %d, now is %v\n", member.ID, member.LocalTime.Format("2006-01-02 15:04:05.000"), member.Heartbeat, time.Now().Format("2006-01-02 15:04:05.000"))
			// fmt.Printf("member %d is suspected, last receive time %s, heartbeat %d, now is %v\n", member.ID, member.LocalTime.Format("2006-01-02 15:04:05.000"), member.Heartbeat, time.Now().Format("2006-01-02 15:04:05.000"))
			fmt.Printf("member %d is suspected\n", member.ID)
			member.Status = "SUSPECTED"
			member.LocalTime = member.LocalTime.Add(suspectTime)
			(*membershipMap)[i] = member
			gossip.PrintMembershipListOnce(membershipMap)
		}
		if time.Since(member.LocalTime) > expireTime &&
			member.ID != myID &&
			member.Status == "SUSPECTED" {
			log.Printf("member %d is failed\n", member.ID)
			fmt.Printf("member %d is failed\n", member.ID)
			f.WriteString(time.Now().Format("15:04:05.999999") + "\n")
			member.Status = "FAILED"
			(*membershipMap)[i] = member
			sendNodeFailMSG(myID, member.ID)
			go cleanup(member.ID, mutex, membershipMap)
			gossip.PrintMembershipListOnce(membershipMap)

		}
	}
	mutex.Unlock()
}

func receiveListUpdate(myHostName string, fileTable *sdfs.FileTable) {
	myAddr := myHostName + ":" + consts.LIST_UPDATE_PORT
	myUDPAddr, err := net.ResolveUDPAddr("udp", myAddr)
	UDPListen, err := net.ListenUDP("udp", myUDPAddr)
	if err != nil {
		log.Println("Error listening ListUpdateMSG ", err)
		return
	}
	log.Println("Listening ListUpdateMSG on port " + consts.LIST_UPDATE_PORT)
	defer UDPListen.Close()
	buffer := make([]byte, 1024*1024)
	for {
		n, _, err := UDPListen.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Error reading ListUpdateMSG", err)
			return
		}
		msg := consts.ListUpdate{}
		err = json.Unmarshal(buffer[:n], &msg)
		if err != nil {
			log.Println("Error Unmarshal ListUpdateMSG", err)
			return
		}
		update := false
		_, exist := (*fileTable)[msg.FileID]
		if exist {
			update = true
		}
		(*fileTable)[msg.FileID] = sdfs.FileRecord{Replica: msg.ReplicaList}
		if update {
			fmt.Printf("file %s list updated %v\n", msg.FileID, msg.ReplicaList)
		} else {
			fmt.Printf("file %s list created %v\n", msg.FileID, msg.ReplicaList)
		}
	}
}

// initGossip delete the membershiplist from leader, send gossip to all members to get the latest membership list
func initGossip(membershipMap *gossip.MembershipMap, myID int64, mutex *sync.Mutex) {
	mutex.Lock()
	// copy membership list for sending
	targetMemberMap := make(gossip.MembershipMap)
	for key, member := range *membershipMap {
		targetMemberMap[key] = member
	}
	// clean up membership list and wait for update
	for key := range *membershipMap {
		if key != myID {
			delete(*membershipMap, key)
		}
	}
	mutex.Unlock()
	// send gossip to all members to get the latest membership list
	for i := 0; i < 2; i++ {
		for _, member := range targetMemberMap {
			udpAddress, _ := net.ResolveUDPAddr("udp", member.Addr)
			conn, err := net.DialUDP("udp", nil, udpAddress)
			if err != nil {
				log.Println("Error sending heartbeat:", err)
				return
			}
			gossip.SendMembershipList(membershipMap, conn, 0)
		}
		time.Sleep(time.Millisecond * 500)
	}
}

// send gossip to other members
func sendGossip(membershipMap *gossip.MembershipMap, myID int64, mutex *sync.Mutex, sus *bool, dropRate *int) {
	initGossip(membershipMap, myID, mutex)
	fileName := "logs/fail_time" + strconv.FormatInt(myID, 10) + ".txt"
	f_for_failure, _ := os.Create(fileName)
	for {
		// update local time
		log.Println("------------------------------------------------------------")
		mutex.Lock()
		log.Printf("start sending lock at %s\n", time.Now().Format("2006-01-02 15:04:05.000"))
		self := (*membershipMap)[myID]
		self.Heartbeat += 1
		self.LocalTime = time.Now()
		(*membershipMap)[myID] = self
		// send gossip
		set := make(map[int64]bool)
		// if there are more than FAN_OUT members, send to FAN_OUT random members
		if len(*membershipMap) > consts.FAN_OUT {
			log.Printf("generate %d random numbers\n", consts.FAN_OUT)
			keys := make([]int64, len(*membershipMap))
			i := 0
			for k := range *membershipMap {
				keys[i] = k
				i++
			}
			rand.Seed(time.Now().UnixNano())
			size := consts.FAN_OUT
			for len(set) < size {
				tmp := rand.Int63n(int64(len(*membershipMap)))
				if keys[tmp] != myID {
					set[keys[tmp]] = true
				}
			}
		} else {
			// membership list is smaller than or equal to FAN_OUT, send to all members except itself
			trueSize := len(*membershipMap) - 1
			if trueSize > 0 {
				log.Printf("generate %d random number\n", trueSize)
				for _, member := range *membershipMap {
					if member.ID != myID {
						set[member.ID] = true
					}
				}
			} else {
				log.Printf("only one member in the group, no need to send gossip\n")
			}
		}
		for key := range set {
			log.Printf("key is %d\n", key)
			membershipMapCopy := make(gossip.MembershipMap)
			for k, v := range *membershipMap {
				if v.Status != "FAILED" {
					membershipMapCopy[k] = v
				}
			}
			go gossip.SendMembershipListAddr(&membershipMapCopy, membershipMapCopy[key].Addr, *dropRate)
			log.Printf("UDP message sent to %s successfully.\n", (*membershipMap)[key].Addr)
			log.Printf("Send my heartbeat %d\n", self.Heartbeat)
		}
		mutex.Unlock()
		log.Printf("stop sending lock at %s\n", time.Now().Format("2006-01-02 15:04:05.000"))
		time.Sleep(consts.GOSSIP_INTERVAL * time.Millisecond)
		// detect & mark & remove failed nodes
		if *sus == true {
			removeExpiredSus(membershipMap, myID, mutex, f_for_failure)
		} else {
			removeExpired(membershipMap, myID, mutex, f_for_failure)
		}
	}
}

// receive gossip from other members, update or add members in the membership list
func receiveGossip(membershipMap *gossip.MembershipMap, myID int64, mutex *sync.Mutex, conn *net.UDPConn, sus *bool) {
	buffer := make([]byte, 2*1024*1024)
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Error reading:", err)
			return
		}
		var newMemberMap gossip.MembershipMap
		json.Unmarshal(buffer[:n], &newMemberMap)
		log.Println("------------------------------------------------------------")
		log.Printf("Received UDP message from %s\n", addr)
		log.Println("received bytes ", n)
		// update member in the membership list
		mutex.Lock()
		for _, newMember := range newMemberMap {
			// skip local member
			if newMember.ID == myID {
				continue
			}
			if newMember.Status == "FAILED" {
				continue
			}
			// update member
			member, exist := (*membershipMap)[newMember.ID]
			if exist {
				if newMember.Heartbeat > member.Heartbeat {
					member.Heartbeat = newMember.Heartbeat
					member.LocalTime = time.Now()

					if member.Status == "ALIVE" && newMember.Status == "SUSPECTED" {
						member.Status = "SUSPECTED"
						member.LocalTime = newMember.LocalTime
						log.Printf("member id:%d addr:%s ALIVE -> SUSPECTED\n", newMember.ID, newMember.Addr)
						fmt.Printf("member id:%d addr:%s ALIVE -> SUSPECTED\n", newMember.ID, newMember.Addr)
					}

					if member.Status == "SUSPECTED" && newMember.Status == "ALIVE" {
						member.Status = "ALIVE"
						log.Printf("member id:%d addr:%s SUSPECTED -> ALIVE\n", newMember.ID, newMember.Addr)
						fmt.Printf("member id:%d addr:%s SUSPECTED -> ALIVE\n", newMember.ID, newMember.Addr)
					}

					(*membershipMap)[newMember.ID] = member
					log.Printf("member id:%d addr:%s is updated\n", newMember.ID, newMember.Addr)
					log.Printf("heartbeat -> %d\n", newMember.Heartbeat)
				} else {
					log.Printf("member id:%d addr:%s is not updated\n", newMember.ID, newMember.Addr)
					log.Printf("received heartbeat %d, local heartbeat %d\n", newMember.Heartbeat, member.Heartbeat)
				}
			} else {
				// new entry
				// mutex.Lock()
				newMember.LocalTime = time.Now()
				(*membershipMap)[newMember.ID] = newMember
				// mutex.Unlock()
				log.Printf("member id:%d add:%s is added\n", newMember.ID, newMember.Addr)
				fmt.Printf("member id:%d add:%s is added\n", newMember.ID, newMember.Addr)
			}

		}
		mutex.Unlock()

	}
}

// connect to leader to get current active members
func initMemberMap(membershipMap *gossip.MembershipMap, myAddr string, introUDPAddr string) (*net.UDPConn, error) {
	myMember := gossip.NewMember(0, myAddr, 0, "ALIVE")
	(*membershipMap)[0] = myMember

	leaderAddr, err := net.ResolveUDPAddr("udp", introUDPAddr)
	connIntroducer, err := net.DialUDP("udp", nil, leaderAddr)

	// Receive UDP message, and initialize membership list
	buffer := make([]byte, 2*1024*1024)
	udpAddress, err := net.ResolveUDPAddr("udp", myAddr)
	conn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		log.Println("Error listening:", err)
		return nil, err
	}

	gossip.SendMembershipList(membershipMap, connIntroducer, 0)
	connIntroducer.Close()

	n, addr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		log.Println("Error reading:", err)
		return nil, err
	}
	for key := range *membershipMap {
		delete(*membershipMap, key)
	}
	json.Unmarshal(buffer[:n], &membershipMap)
	log.Printf("Received UDP message from %s\n %+v", addr, membershipMap)

	// setup current time
	for _, member := range *membershipMap {
		member.LocalTime = time.Now()
	}
	return conn, nil
}
