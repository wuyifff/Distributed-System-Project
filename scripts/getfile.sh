#!/bin/bash

cd ..
echo "please enter the machine number to scp"
read node
echo "please enter the relative file path"
read path
echo "the command will be: scp yifei23@fa23-cs425-$node.cs.illinois.edu:/home/yifei23/cs425mp3/$path ."
scp yifei23@fa23-cs425-$node.cs.illinois.edu:/home/yifei23/cs425mp3/$path .