package sdfs

import (
	"strings"
)

func LsFolder(sdfs_dir string, fileTable *FileTable) []string {
	var files []string
	for key := range *fileTable {
		if strings.HasPrefix(key, sdfs_dir) {
			files = append(files, key)
		}
	}
	return files
}
