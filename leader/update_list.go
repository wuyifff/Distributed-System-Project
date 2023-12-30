package main

import (
	"encoding/json"
	"log"
	"main/consts"
	"main/gossip"
	"main/sdfs"
	"math/rand"
	"net"
	"strings"
	"time"
)

func replicaUpdate(fileTable *sdfs.FileTable, failedNode int64, membershipMap *gossip.MembershipMap) {
	time.Sleep(8 * time.Second)
	rand.NewSource(time.Now().UnixMilli())
	log.Printf("Replicate %d node's replica\n", failedNode)
	for fileName, replicaList := range *fileTable {
		ReplicaMap := make(map[int64]bool)
		del := -1
		var index int64
		for n, node := range replicaList.Replica {
			// log.Printf("n = %d node = %d faildNode = %d", n, node, failedNode)
			if node == failedNode {
				del = n
				log.Printf("file in failed node %d need to be replicated : %s\n", failedNode, fileName)
			}
			ReplicaMap[node] = true
		}
		if del != -1 {
			//nodeList := make([]int64, len(*membershipMap))
			//for id, _ := range *membershipMap {
			//	nodeList = append(nodeList, id)
			//}
			//for {
			//	index = nodeList[int64(rand.Int()%(len(*membershipMap)))]
			//	member, exist := (*membershipMap)[index]
			//	if !ReplicaMap[index] && index != failedNode && exist && member.Status == "ALIVE" {
			//		break
			//	}
			//
			//}
			for {
				index = rand.Int63()%10 + 1
				member, exist := (*membershipMap)[index]
				if !ReplicaMap[index] && index != failedNode && exist && member.Status == "ALIVE" {
					break
				}

			}
			log.Printf("file %s in failed node %d will be replicated in : %d\n", fileName, failedNode, index)
			// send replica message to node index
			member, _ := (*membershipMap)[index]
			memberIP := strings.Split(member.Addr, ":")[0]

			msg := consts.LeaderMsg{
				MsgType: consts.LEADER_MSG_REPLICA,
				FileID:  fileName,
			}

			conn, err := net.Dial("udp", memberIP+":"+consts.GET_MSG_PORT)
			if err != nil {
				log.Println("error dialing UDP replicaUpdate ", err)
				return
			}

			marshal, err := json.Marshal(msg)
			if err != nil {
				log.Println("error marshalling replicaUpdate ", err)
				return
			}
			_, err = conn.Write(marshal)
			if err != nil {
				log.Println("error sending replicaUpdate ", err)
				return
			}
			log.Println("replica message send to node : ", index, " ", memberIP+":"+consts.GET_MSG_PORT)

			buffer := make([]byte, 1024*1024)

			n, err := conn.Read(buffer)
			if err != nil {
				log.Println("error reading response replicaUpdate ", err)
				return
			}
			resp := consts.LeaderResp{}
			err = json.Unmarshal(buffer[:n], &resp)
			if err != nil {
				log.Println("error Unmarshal response replicaUpdate ", err)
				return
			}

			if resp.RespType == consts.LEADER_RESP_OK {
				replicaList.Replica[del] = index
				log.Println("replica message response received", index)
			} else {
				log.Println("Unexpected Error replicaUpdate: code not OK ", err)
			}

			sendNewList(fileName, replicaList.Replica, membershipMap)
		}

	}
}
