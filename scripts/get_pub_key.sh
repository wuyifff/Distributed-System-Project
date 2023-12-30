#for i in {6106..6110}
#do
#    scp linpuh2@fa23-cs425-$i.cs.illinois.edu:/home/linpuh2/.ssh/pub_key_$i.pub ./pub_key_$i
#    cat pub_key_$i >> authorized_keys
#done
cat ~/.ssh/id_rsa.pub >> authorized_keys
for i in {6106..6110}
do
    scp authorized_keys linpuh2@fa23-cs425-$i.cs.illinois.edu:/home/linpuh2/.ssh/authorized_keys
done

