# Failure Detector, SDFS, MapleJuice & SQL

implemented in GO.

Failure Detector use membership list and gossip algorithm.

SDFS ensures 3 replications of the original file, and backup automatically when nodes fail.

MapleJuice is the same with MapReduce.

## Commands

test locally

```shell
go run ./leader localhost:8080
go run ./worker localhost:10001
go run ./worker localhost:10002
go run ./worker localhost:10003
```

test in remote machine (for each node), say we ssh to 5 different machine

```shell
ssh yifei23@fa23-cs425-6101.cs.illinois.edu
cd ~/cs425mp4; go run ./leader fa23-cs425-6110.cs.illinois.edu:10000
cd ~/cs425mp4; go run ./worker fa23-cs425-6101.cs.illinois.edu:10001
cd ~/cs425mp4; go run ./worker fa23-cs425-6102.cs.illinois.edu:10001
cd ~/cs425mp4; go run ./worker fa23-cs425-6103.cs.illinois.edu:10001
cd ~/cs425mp4; go run ./worker fa23-cs425-6104.cs.illinois.edu:10001
cd ~/cs425mp4; go run ./worker fa23-cs425-6105.cs.illinois.edu:10001
```

here are some commands below

```shell
list_self list self ID
sus enable suspicion
no_sus disable suspicion
list_mem: list the membership list
put localfilename sdfsfilename
get sdfsfilename localfilename
delete sdfsfilename
ls sdfsfilename
store
table
mutiread sdfsfilename VM1 VM2 ...
maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory> <extra input # if it is null>
juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> <delete_input={0,1}>
SELECT ALL FROM Dataset WHERE <regex condition>
```

say we want to upload a file to the system

```shell
put 1.tar.gz 1.txt
```

and the file 1.txt will be replicated in the system

## directory

file UPLOADED in the system is stored in the directory `/var/tmp/{nodeID}/{filename}`

file GET from the system is stored in the directory `/home/{username}/cs425mp4/{filename}`

file GENERATE to insert in stored in the directory `/home/{username}/{filename}`

## Map Reduce

test map

```shell
put traffic.csv traffic.csv
maple maple1.exe 3 o1 traffic.csv Fiber
juice juice1.exe 3 o1 o2 0
maple maple2.exe 3 o3 o2 #
juice juice2.exe 1 o3 o4 0

or

put traffic.csv traffic.csv
testall traffic.csv Fiber
```

test SQL

```shell
put traffic.csv traffic.csv
SELECT ALL FROM traffic.csv WHERE "Video,Radio"
SELECT ALL FROM traffic.csv WHERE "Video.*Radio"
SELECT ALL FROM traffic.csv WHERE "Video|Radio"
```

## Replace rules

Due to key should not be empty or space, and we not allow '/' appear in our file name, we do following replace:

```shell
"" -> "@"
" " -> "$"
"/" -> "%"
```

test big files

```shell
put traffic_head.csv traffic.csv
SELECT ALL FROM traffic.csv traffic.csv WHERE OBJECTID = OBJECTID
SELECT ALL FROM traffic.csv traffic.csv WHERE Countdown_ = Countdown_
```
