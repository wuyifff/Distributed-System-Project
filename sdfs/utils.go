package sdfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"main/gossip"
	"math/rand"
	"os"
	"sort"
	"time"
)

// generate SHA-256 hash of file
func GetHash(file *os.File) string {
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		fmt.Println("Error generating SHA-256 hash:", err)
		return ""
	}
	// result in hex
	hashInBytes := hash.Sum(nil)
	hashInString := hex.EncodeToString(hashInBytes)
	// reset file pointer
	file.Seek(0, 0)
	return hashInString
}

// convert hash to replica node id list
func GetReplica(membershipMap *gossip.MembershipMap, myID int64) []int64 {
	replica := [4]int64{-1, -1, -1, -1}
	length := 0
	livingNodes := membershipMap.GetLivingNodeNum()
	if livingNodes < 4 {
		length = livingNodes
	} else {
		length = 4
	}
	replica[0] = myID
	rand.Seed(time.Now().UnixNano())
	keys := make([]int64, len(*membershipMap))
	i := 0
	for k := range *membershipMap {
		keys[i] = k
		i++
	}
	for i := 1; i < length; i++ {
		tmp := myID
		for {
			index := rand.Int63n(int64(len(*membershipMap)))
			tmp = keys[index]
			if tmp == replica[0] || tmp == replica[1] || tmp == replica[2] || tmp == replica[3] {
				continue
			} else {
				replica[i] = tmp
				break
			}
		}
	}
	slice := replica[:]
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] > slice[j]
	})

	return slice
}

func PrintFileTable(fileTable *FileTable) {
	if len(*fileTable) == 0 {
		fmt.Println("File table is empty")
		return
	}
	fmt.Printf("+--File Name--+----Replica----+\n")
	for key, value := range *fileTable {
		fmt.Printf("|%-13s|   %v   |\n", key, value.Replica)
	}
	fmt.Printf("+-------------+---------------+\n")
}
