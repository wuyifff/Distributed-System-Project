# !/bin/bash
cd ..
for i in {6101..6102}
do
    ssh yifei23@fa23-cs425-$i.cs.illinois.edu << EOF
        kill \$(lsof -t -i:10001)
        exit
EOF
echo "Server killed on fa23-cs425-$i.cs.illinois.edu:10001"
done