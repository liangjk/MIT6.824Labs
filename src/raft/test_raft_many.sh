cd ~/MIT6.824/golabs/src/raft/
for i in {1..20}; do
    for j in {1..80}; do
        echo -n "-"
    done
    echo -e "\nTest $i without race flag"
    go test -timeout 15m
    for j in {1..80}; do
        echo -n "*"
    done
    echo -e "\nTest $i with race flag"
    go test -timeout 20m
done | tee test.log