package sdfs

import (
	"fmt"
	"os"

	"github.com/codingsince1985/checksum"
)

func Store(myID int64, localFileTable *LocalFileTable) {
	if len(*localFileTable) == 0 {
		fmt.Println("No file stored in this machine")
		return
	} else {
		fmt.Println("sdfsFileName   localFileName   length   checksum ")
		for sdfsFileName, localFileName := range *localFileTable {
			stat, err := os.Stat(localFileName)
			if err != nil {
				fmt.Println("error retrieving file", err)
				return
			}
			cksum, _ := checksum.MD5sum(localFileName)
			fmt.Printf("%s,  %s,  %d,  %s \n", sdfsFileName, localFileName, stat.Size(), cksum)

		}
		fmt.Println("--------------------------")
	}

}
