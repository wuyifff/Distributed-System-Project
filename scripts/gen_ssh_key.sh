for i in {6106..6110}
do
    ssh linpuh2@fa23-cs425-$i.cs.illinois.edu << EOF
            cd ~/.ssh
            ssh-keygen
            pub_key_$i
EOF
done