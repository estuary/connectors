package main

import (
	"bytes"
	"context"
	"encoding/asn1"
	"encoding/binary"
	"net"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/stretchr/testify/require"
)

func TestMountUsesHostName(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	previousDial := smbDialContext
	smbDialContext = func(context.Context, string, string) (net.Conn, error) {
		// Simulate a resolved address that differs from the configured hostname.
		return mountTestConn{Conn: client, remoteAddr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 445}}, nil
	}
	defer func() { smbDialContext = previousDial }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		store, err := newStore(ctx, config{
			Host: "fs01.corp.example", Port: 445,
			Share: "exports", Username: "test", Password: "test",
		})
		if store != nil {
			store.Close()
		}
		result <- err
	}()
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))

	request, err := readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, uint16(0), binary.LittleEndian.Uint16(request[12:]))
	negotiate := make([]byte, 64)
	binary.LittleEndian.PutUint16(negotiate, 65)
	binary.LittleEndian.PutUint16(negotiate[2:], 1) // Signing enabled.
	binary.LittleEndian.PutUint16(negotiate[4:], 0x210)
	for _, offset := range []int{28, 32, 36} {
		binary.LittleEndian.PutUint32(negotiate[offset:], 65536)
	}
	writeSmbPacket(t, server, smbResponse(request, 0, negotiate))

	request, err = readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, uint16(1), binary.LittleEndian.Uint16(request[12:]))
	challenge := make([]byte, 52)
	copy(challenge, "NTLMSSP\x00")
	binary.LittleEndian.PutUint32(challenge[8:], 2)
	binary.LittleEndian.PutUint32(challenge[20:], 0x00888215)
	copy(challenge[24:], "12345678")
	binary.LittleEndian.PutUint16(challenge[40:], 4)
	binary.LittleEndian.PutUint16(challenge[42:], 4)
	binary.LittleEndian.PutUint32(challenge[44:], 48) // Empty target-info list.
	token, err := asn1.MarshalWithParams(struct {
		State     asn1.Enumerated       `asn1:"explicit,tag:0"`
		Mechanism asn1.ObjectIdentifier `asn1:"explicit,tag:1"`
		Token     []byte                `asn1:"explicit,tag:2"`
	}{1, asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 311, 2, 2, 10}, challenge}, "explicit,tag:1")
	require.NoError(t, err)
	setup := make([]byte, 8+len(token))
	binary.LittleEndian.PutUint16(setup, 9)
	binary.LittleEndian.PutUint16(setup[4:], 72)
	binary.LittleEndian.PutUint16(setup[6:], uint16(len(token)))
	copy(setup[8:], token)
	writeSmbPacket(t, server, smbResponse(request, 0xc0000016, setup))

	request, err = readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, uint16(1), binary.LittleEndian.Uint16(request[12:]))
	setup = make([]byte, 8)
	binary.LittleEndian.PutUint16(setup, 9)
	writeSmbPacket(t, server, smbResponse(request, 0, setup))

	request, err = readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, uint16(3), binary.LittleEndian.Uint16(request[12:]), "expected SMB2 TREE_CONNECT")
	offset := int(binary.LittleEndian.Uint16(request[68:]))
	length := int(binary.LittleEndian.Uint16(request[70:]))
	name := make([]uint16, length/2)
	require.NoError(t, binary.Read(bytes.NewReader(request[offset:offset+length]), binary.LittleEndian, name))
	require.Equal(t, `\\fs01.corp.example\exports`, string(utf16.Decode(name)))

	// The request proves the mount path; closing the server ends the pending mount.
	require.NoError(t, server.Close())
	select {
	case err := <-result:
		require.ErrorContains(t, err, `mounting share "exports"`)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type mountTestConn struct {
	net.Conn
	remoteAddr net.Addr
}

func (c mountTestConn) RemoteAddr() net.Addr { return c.remoteAddr }
