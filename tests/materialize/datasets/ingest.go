package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
	"google.golang.org/grpc"
)

type config struct {
	serverAddr   string
	captureName  string
	bindingNum   int
	dataFilePath string
}

func parseConfig() *config {
	var cfg config
	flag.StringVar(&cfg.serverAddr, "server_address", "localhost:9000", "address of the consumer server for data ingestion.")
	flag.StringVar(&cfg.captureName, "capture", "capture/ingest", "name of the catalog capture to receive data.")
	flag.IntVar(&cfg.bindingNum, "binding_num", 0, "the number of binding for data ingestion.")
	flag.StringVar(&cfg.dataFilePath, "data_file_path", "./data.jsonl", "path to th jsonl file that contains a list of json data to be ingested.")

	flag.Parse()
	return &cfg
}

type pushRpc struct {
	rpc  capture.Runtime_PushClient
	conn *grpc.ClientConn
}

func newPushRpc(ctx context.Context, serverAddr string) (*pushRpc, error) {
	dialer := func(addr string, t time.Duration) (net.Conn, error) {
		return net.Dial("unix", addr)
	}

	if conn, err := grpc.Dial(serverAddr, grpc.WithInsecure(), grpc.WithDialer(dialer)); err != nil {
		return nil, fmt.Errorf("fail to dial: %w", err)
	} else if rpc, err := capture.NewRuntimeClient(conn).Push(ctx); err != nil {
		return nil, fmt.Errorf("failed to create Push rpc: %w", err)
	} else {
		return &pushRpc{
			conn: conn,
			rpc:  rpc,
		}, nil
	}
}

func (p *pushRpc) Open(captureName string) error {
	return p.rpc.Send(&capture.PushRequest{
		Open: &capture.PushRequest_Open{
			Capture: flow.Capture(captureName),
		},
	})
}

func (p *pushRpc) SendDocuments(dataFilePath string, bindingNum int) error {
	var file, err = os.Open(dataFilePath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", dataFilePath, err)
	}
	var scanner = bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	var arena flow.Arena
	var docs []flow.Slice
	for scanner.Scan() {
		docs = append(docs, arena.Add(scanner.Bytes()))
	}

	if err := scanner.Err(); err != nil {
		fmt.Errorf("scan input: %w", err)
	}

	return p.rpc.Send(&capture.PushRequest{
		Documents: &capture.Documents{
			Binding:  uint32(bindingNum),
			Arena:    arena,
			DocsJson: docs,
		},
	})
}

func (p *pushRpc) Checkpoint() error {
	return p.rpc.Send(&capture.PushRequest{
		Checkpoint: &flow.DriverCheckpoint{},
	})
}

func (p *pushRpc) Acknowledge() error {
	var resp, err = p.rpc.Recv()
	if err != nil {
		return fmt.Errorf("failed in receiving response, %+v", err)
	} else if err = resp.Validate(); err != nil {
		return fmt.Errorf("failed in validating response, %+v", err)
	}
	return nil
}

func (p *pushRpc) Close() {
	p.conn.Close()
}

func logAndExit(err error) {
	log.Fatalf("execution failed: %v", err)
	os.Exit(1)
}

func main() {
	var cfg = parseConfig()

	var ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var rpc, err = newPushRpc(ctx, cfg.serverAddr)
	if err != nil {
		logAndExit(fmt.Errorf("execution failed: %w", err))
	}
	defer rpc.Close()

	if err := rpc.Open(cfg.captureName); err != nil {
		logAndExit(fmt.Errorf("open request: %w", err))
	} else if err := rpc.SendDocuments(cfg.dataFilePath, cfg.bindingNum); err != nil {
		logAndExit(fmt.Errorf("send documents: %w", err))
	} else if err := rpc.Checkpoint(); err != nil {
		logAndExit(fmt.Errorf("checkpoint: %w", err))
	} else if err := rpc.Acknowledge(); err != nil {
		logAndExit(fmt.Errorf("acknowledge: %w", err))
	}
}
