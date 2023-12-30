#!/bin/bash
cd executable
make clean
cd ..
rm -rf ../cs425mp4.zip
zip -r ../cs425mp4.zip ./
output_dir="/home/yifei23"
# Define the range of machine numbers (from 6101 to 6110)
start=6100
# for i in {6101..6110}
for i in 6110 {6101..6105} 
do
    index=$((i - start))
    # file_name="$index.txt"
    # file_name="big.txt"
    # echo ${file_name}
    # Copy cs425mp4.zip to the remote machine
    scp ../cs425mp4.zip yifei23@fa23-cs425-$i.cs.illinois.edu:/home/yifei23/
    # SSH into the remote machine
    ssh yifei23@fa23-cs425-$i.cs.illinois.edu << EOF
        rm -rf cs425mp4
        rm -rf /var/tmp/$index
        unzip cs425mp4.zip -d cs425mp4
        # cp cs425mp4/executable/* executable
        cd executable
        make
        cp *.exe ../cs425mp4
        exit
EOF
done