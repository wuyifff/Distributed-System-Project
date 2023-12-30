package gossip

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

func PrintMembershipList(membershipMap *MembershipMap, mutex *sync.Mutex) {
	for {
		mutex.Lock()
		members := make([]Member, 0, len(*membershipMap))
		for _, member := range *membershipMap {
			members = append(members, member)
		}
		mutex.Unlock()
		// Sort the slice by ID
		sort.Slice(members, func(i, j int) bool {
			return members[i].ID < members[j].ID
		})
		log.Printf("+------+------------------------------------------+------------+------------+\n")
		log.Printf("|  ID  |                   Addr                   | Heartbeat  |   Status   |\n")
		log.Printf("+------+------------------------------------------+------------+------------+\n")

		for _, member := range members {
			log.Printf("| %-4d | %-40s | %-10d | %-10s |\n",
				member.ID, member.Addr, member.Heartbeat, member.Status)
		}
		log.Printf("+------+------------------------------------------+------------+------------+\n")
		time.Sleep(5000 * time.Millisecond)
	}
}

func PrintMembershipListOnce(membershipMap *MembershipMap) {
	membershipMapCopy := make(MembershipMap)
	for k, v := range *membershipMap {
		membershipMapCopy[k] = v
	}
	members := make([]Member, 0, len(membershipMapCopy))
	for _, member := range membershipMapCopy {
		members = append(members, member)
	}
	// Sort the slice by ID
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	fmt.Printf("+------+------------------------------------------+------------+------------+\n")
	fmt.Printf("|  ID  |                   Addr                   | Heartbeat  |   Status   |\n")
	fmt.Printf("+------+------------------------------------------+------------+------------+\n")

	for _, member := range members {
		fmt.Printf("| %-4d | %-40s | %-10d | %-10s |\n",
			member.ID, member.Addr, member.Heartbeat, member.Status)
	}
	fmt.Printf("+------+------------------------------------------+------------+------------+\n")
}
