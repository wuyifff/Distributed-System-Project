package sdfs

import "strings"

func SDFSFileNameToLocalFileName(sdfsFileName string) string {
	result := strings.Replace(sdfsFileName, "/", "%", -1)
	return result
}

func LocalFileNameToSDFSFileName(localFileName string) string {
	result := strings.Replace(localFileName, "%", "/", -1)
	return result
}
