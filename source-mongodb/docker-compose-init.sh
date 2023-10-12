function mongosh() {
	echo "$@" | docker exec -i source-mongodb-db-1 mongosh -u flow -p flow admin
}

mongosh 'rs.initiate()'
sleep 3
mongosh 'cfg = rs.conf(); cfg.members[0].host="localhost:27017"; rs.reconfig(cfg);'
