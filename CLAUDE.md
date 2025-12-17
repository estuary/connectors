# Build Docker image locally

The most quick way to build a docker image of a connector is to edit its Dockerfile, comment out the `go test` and `go mod` lines in Dockerfile, and replace `go build` with `COPY ./connector ./`, and use the following set of commands to build the local image:
```
cd $connector; GOARCH=amd64 GOOS=linux go build -tags nozstd -v -o ../connector && cd .. && ./build-local.sh $connector
```

# Testing

- Always provide `TEST_DATABASE=yes UPDATE_SNAPSHOTS=true` when running `go test -v ./$connector`
- For connectors which have a corresponding folder in tests/ or tests/materialize/, also run integration tests using the following command: `CONNECTOR=materialize-mysql VERSION=local ./tests/materialize/run.sh`.
- To run integration tests, must build the docker image locally.
