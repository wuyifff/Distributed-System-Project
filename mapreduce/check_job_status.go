package mapreduce

import (
	"fmt"
	"main/gossip"
	"main/sdfs"
	"sync"
)

func checkJobStatus(requestRecord *[]MapReduceMsg, jobMaps map[int]map[int64][]JobInfo, mu *sync.Mutex, fileTable *sdfs.FileTable, membershipMap *gossip.MembershipMap, downChannel chan int64) {
	for {
		downNode := <-downChannel
		for _, mp := range jobMaps {
			for nodeID, joblist := range mp {
				if nodeID == downNode {
					var jobNeedAssign []JobInfo
					for _, job := range joblist {
						if job.fileName == "" {
							jobNeedAssign = append(jobNeedAssign, job)
						}
					}
					for _, job := range jobNeedAssign {
						// assign the job to other node
						substutionID := membershipMap.LivingNodeShuffle()[0]
						sendJob(substutionID, job.originNode, job.jobID, (*requestRecord)[job.jobID-1], membershipMap, fileTable)
						fmt.Println("------------------------")
						fmt.Printf("Node %d's jobID %d originate from %d replaced by node %d\n", downNode, job.jobID, job.originNode, substutionID)
						mu.Lock()
						delete(mp, nodeID)
						mp[substutionID] = append(mp[substutionID], job)
						mu.Unlock()
					}
					break
				}
			}
		}
	}
}
