cd ~/MIT6.824/golabs/src/paxos/
for i in {1..200}; do
    for j in {1..80}; do
        echo -n "-"
    done
    echo -e "\nTest $i without race flag"
    time go test -timeout 20m
    for j in {1..80}; do
        echo -n "*"
    done
    echo -e "\nTest $i with race flag"
    time go test -race -timeout 20m
done 2>&1 | tee test.log