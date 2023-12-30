package main

import (
	"encoding/json"
	"log"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"net"
)

func receiveUpdateMsg(fileTable *sdfs.FileTable, downVote *map[int64]int64, membershipMap *gossip.MembershipMap, conn *net.UDPConn, downChannel chan int64) {
	buffer := make([]byte, 4096)
	// Receive UDP message, and make membership list
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("receiveUpdateMsg Error reading:", err)
			return
		}
		var leaderMessage consts.LeaderMsg
		err = json.Unmarshal(buffer[:n], &leaderMessage)
		// fmt.Printf("Get leader message: %+v\n", leaderMessage)
		if err != nil {
			log.Println("Error unmarshall leader message:", err)
			return
		}
		switch leaderMessage.MsgType {
		case consts.LEADER_MSG_DOWN:
			if (*downVote)[leaderMessage.MachID] <= consts.DOWN_VOTE_THRESHOLD {
				// print("down cnt from ", strconv.Itoa(int((*downVote)[leaderMessage.MachID])))
				(*downVote)[leaderMessage.MachID]++
				// println(" to ", strconv.Itoa(int((*downVote)[leaderMessage.MachID])))
			}

			if (*downVote)[leaderMessage.MachID] == consts.DOWN_VOTE_THRESHOLD {
				downChannel <- leaderMessage.MachID
				member, _ := (*membershipMap)[leaderMessage.MachID]
				member.Status = "FAILED"
				(*membershipMap)[leaderMessage.MachID] = member
				log.Println("Member ", leaderMessage.MachID, " is Failed")
				go replicaUpdate(fileTable, leaderMessage.MachID, membershipMap)
			}

		case consts.LEADER_MSG_GET:
			record, exist := (*fileTable)[leaderMessage.FileID]
			list := make([]int64, 0)
			tmp := consts.LeaderResp{
				RespType:    consts.LEADER_RESP_OK,
				ReplicaList: list,
			}
			if exist {
				tmp.ReplicaList = record.Replica
			} else {
				tmp.RespType = consts.LEADER_RESP_NO
			}

			resp, _ := json.Marshal(tmp)
			_, err := conn.WriteToUDP(resp, remoteAddr)
			if err != nil {
				log.Println("error sending reply to ", remoteAddr)
				return
			}

		case consts.LEADER_MSG_PUT:

			list := make([]int64, 0)
			list = leaderMessage.Replica
			sdfsFileName := leaderMessage.FileID
			sendNewList(sdfsFileName, list, membershipMap)
			if err != nil {
				log.Println("error sending reply to ", remoteAddr)
				return
			}
			fileRecord := sdfs.FileRecord{
				Replica: list,
			}
			_, exist := (*fileTable)[leaderMessage.FileID]
			if !exist {
				(*fileTable)[leaderMessage.FileID] = fileRecord
			}

		case consts.LEADER_MSG_DEL:
			list := make([]int64, 0)
			delete(*fileTable, leaderMessage.FileID)
			sendNewList(leaderMessage.FileID, list, membershipMap)

		default:
			log.Println("Unknown message type")
		}
	}
}
