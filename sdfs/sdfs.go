package sdfs

type FileRecord struct {
	Replica []int64 `json:"replica"`
}

type FileTable map[string]FileRecord
type LocalFileTable map[string]string

func IsFileExist(fileTable *FileTable, sdfsDir string) bool {
	_, exist := (*fileTable)[sdfsDir]
	var files []string
	if exist {
		return true
	} else {
		files = LsFolder(sdfsDir, fileTable)
	}
	return len(files) > 0
}
