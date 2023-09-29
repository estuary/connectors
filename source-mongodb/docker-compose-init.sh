function mongosh() {
	echo "$@" | docker exec -i source-mongodb-mongo-1 mongosh -u flow -p flow
}

mongosh 'rs.initiate()'
mongosh 'db.createUser({"user": "flow", "pwd": "flow", "roles": ["readAnyDatabase", "readWrite"]})'
sleep 3
mongosh 'cfg = rs.conf(); cfg.members[0].host="localhost:27017"; rs.reconfig(cfg);'
