package consts

// ip and port configuration consts
const (
	LEADER_HOSTNAME = "fa23-cs425-6110.cs.illinois.edu"
	LEADER_PORT     = "10000"
	// LEADER_HOSTNAME  = "localhost"
	// LEADER_PORT      = "8080"
	LEADER_ADDR      = LEADER_HOSTNAME + ":" + LEADER_PORT
	LEADER_MSG_PORT  = "11111"
	LEADER_MSG_ADDR  = LEADER_HOSTNAME + ":" + LEADER_MSG_PORT
	LIST_UPDATE_PORT = "22222"
	GET_MSG_PORT     = "33333"
	MR_PORT          = "44444"
	//LEADER_PORT = "20000"
	//// LEADER_HOSTNAME  = "localhost"
	//// LEADER_PORT      = "8080"
	//LEADER_ADDR      = LEADER_HOSTNAME + ":" + LEADER_PORT
	//LEADER_MSG_PORT  = "11112"
	//LEADER_MSG_ADDR  = LEADER_HOSTNAME + ":" + LEADER_MSG_PORT
	//LIST_UPDATE_PORT = "22223"
	//GET_MSG_PORT     = "22224"
	//MR_PORT          = "22225"

	LEADER_ID = 0
)

// sdfs file system consts
const (
	FILE_STORE_PATH = "/var/tmp"
)

// gossip consts
const (
	DEFALUT_SUS         = false
	DEFAULT_DROP_RATE   = 0
	GOSSIP_INTERVAL     = 200 // ms
	SUSPECT_TIME        = 3
	EXPIRE_TIME         = 10
	DELETE_TIME         = 5
	FAN_OUT             = 2
	DOWN_VOTE_THRESHOLD = 2
)
