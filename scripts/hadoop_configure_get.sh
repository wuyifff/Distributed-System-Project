# scp yifei23@fa23-cs425-6110.cs.illinois.edu:/usr/local/hadoop/etc/hadoop/* hadoop_files

mkdir hadoop_files
for i in {6106..6110} 
do
    mkdir hadoop_files/$i
    scp yifei23@fa23-cs425-$i.cs.illinois.edu:/usr/local/hadoop/etc/hadoop/hadoop-env.sh hadoop_files/$i/hadoop-env.sh
    scp yifei23@fa23-cs425-$i.cs.illinois.edu:/usr/local/hadoop/etc/hadoop/yarn-env.sh hadoop_files/$i/yarn-env.sh
    scp yifei23@fa23-cs425-$i.cs.illinois.edu:/usr/local/hadoop/etc/hadoop/core-site.xml hadoop_files/$i/core-site.xml
    scp yifei23@fa23-cs425-$i.cs.illinois.edu:/usr/local/hadoop/etc/hadoop/hdfs-site.xml hadoop_files/$i/hdfs-site.xml
    scp yifei23@fa23-cs425-$i.cs.illinois.edu:/usr/local/hadoop/etc/hadoop/mapred-site.xml hadoop_files/$i/mapred-site.xml
    scp yifei23@fa23-cs425-$i.cs.illinois.edu:/usr/local/hadoop/etc/hadoop/yarn-site.xml hadoop_files/$i/yarn-site.xml
done