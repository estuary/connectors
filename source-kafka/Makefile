# Test Cluster Management Tasks:

topic=todo-list
partitions=4

.PHONY: test_create
test_create:
	kafkactl create topic ${topic} --partitions ${partitions}

.PHONY: test_destroy
test_destroy:
	kafkactl delete topic ${topic}

.PHONY: test_seed
test_seed:
# TODO: Figure out why looping does not want to work.
	cat ./tests/fixtures/${topic}.json | jq -c "map(select(.userId % 4 == 0)) | .[]" | kafkactl produce ${topic} -p 0
	cat ./tests/fixtures/${topic}.json | jq -c "map(select(.userId % 4 == 1)) | .[]" | kafkactl produce ${topic} -p 1
	cat ./tests/fixtures/${topic}.json | jq -c "map(select(.userId % 4 == 2)) | .[]" | kafkactl produce ${topic} -p 2
	cat ./tests/fixtures/${topic}.json | jq -c "map(select(.userId % 4 == 3)) | .[]" | kafkactl produce ${topic} -p 3

.PHONY: test_setup
test_setup: test_create test_seed
