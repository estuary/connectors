FROM golang:latest

COPY ./go.mod ./
COPY ./go.sum ./
COPY ./query.go ./
ENV GOPATH=/nonexistant

CMD ["go", "run", "query.go"]
