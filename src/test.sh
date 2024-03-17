for i in {1..200}; do
	for j in {1..80}; do
		echo -n "-"
	done
	echo -e "\nTest $i without race flag"

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest Raft"
	time go test ./raft -v

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest KVRaft"
	time go test ./kvraft -timeout 15m -v

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest ShardCtrler"
	time go test ./shardctrler -v

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest ShardKV"
	time go test ./shardkv -v

	for j in {1..80}; do
		echo -n "*"
	done
	echo -e "\nTest $i with race flag"

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest Raft Race"
	time go test ./raft -race -timeout 20m -v

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest KVRaft Race"
	time go test ./kvraft -race -timeout 20m -v

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest ShardCtrler Race"
	time go test ./shardctrler -race -v

	for j in {1..80}; do
		echo -n "~"
	done
	echo -e "\nTest ShardKV Race"
	time go test ./shardkv -race -timeout 20m -v
done 2>&1 | tee test.log
