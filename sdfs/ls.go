package sdfs

import (
	"fmt"
	"main/gossip"
	"strings"
)

func Ls(sdfsFileName string, fileTable *FileTable, membershipMap *gossip.MembershipMap) {
	replica := (*fileTable)[sdfsFileName].Replica
	var list []string
	for _, nodeId := range replica {
		if _, exists := (*membershipMap)[nodeId]; exists {
			Addr := (*membershipMap)[nodeId].Addr
			list = append(list, strings.Split(Addr, ":")[0])
		}
	}
	if len(list) == 0 {
		fmt.Printf("No replica of %s stored in SDFS\n", sdfsFileName)
		return
	} else {
		fmt.Printf("Replica of %s stored in SDFS:\n", sdfsFileName)
		for _, ip := range list {
			fmt.Println(ip)
		}
		fmt.Println()
	}
}
