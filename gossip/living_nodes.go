package gossip

import (
	"math/rand"
	"time"
)

// GetLivingNodeNum returns the number of living nodes
func (m MembershipMap) GetLivingNodeNum() int {
	count := 0
	for _, member := range m {
		if member.Status == "ALIVE" {
			count++
		}
	}
	return count
}

// GetRandomLivingNodes returns a list of random living nodes
func (m MembershipMap) LivingNodeShuffle() []int64 {
	nodeList := make([]int64, 0)
	for id, member := range m {
		if member.Status == "ALIVE" {
			nodeList = append(nodeList, id)
		}
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(nodeList), func(i, j int) {
		nodeList[i], nodeList[j] = nodeList[j], nodeList[i]
	})
	return nodeList
}
