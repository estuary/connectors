# Flow MongoDB Source Connector

This is a Flow capture connector which captures documents and change events from a
[MongoDB](https://www.mongodb.com/) database.


# Database tests
To run database tests, start the docker compose and initiate it:

```
docker compose -f source-mongodb/docker-compose.yaml up
bash source-mongodb/docker-compose-init.sh
```

Then you can run database tests:
```
TEST_DATABASE=yes go test -tags nozstd -v ./source-mongodb
```
