# !/bin/bash
cd executable
make
./maple1.exe ../traffic.csv o1.txt
./juice1.exe o1.txt o2.txt
./maple2.exe o2.txt o3.txt
./juice2.exe o3.txt o4.txt
make clean
cat o4.txt
rm *.txt