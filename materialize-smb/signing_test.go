package main

import (
	"context"
	"encoding/asn1"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestStoreRejectsUnsignedSessions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		flags    uint16
		validate bool
	}{
		{name: "guest", flags: 0x1},
		{name: "anonymous", flags: 0x2},
		{name: "guest and anonymous", flags: 0x3},
		{name: "validate guest", flags: 0x1, validate: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			previousDial := smbDialContext
			smbDialContext = func(context.Context, string, string) (net.Conn, error) { return client, nil }
			defer func() { smbDialContext = previousDial }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := config{
				Host: "smb.example.test", Port: 445,
				Share: "data", Username: "test", Password: "bad-password", UploadInterval: "5m",
			}
			raw, err := json.Marshal(cfg)
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() {
				if tt.validate {
					_, err := driver.Validate(ctx, &pm.Request_Validate{ConfigJson: raw})
					result <- err
					return
				}
				store, err := newStore(ctx, cfg)
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
			// The challenge has no guest flags; only the final response sets them.
			writeSmbPacket(t, server, smbResponse(request, 0xc0000016, setup))

			request, err = readSmbPacket(server)
			require.NoError(t, err)
			require.Equal(t, uint16(1), binary.LittleEndian.Uint16(request[12:]))
			setup = make([]byte, 8)
			binary.LittleEndian.PutUint16(setup, 9)
			binary.LittleEndian.PutUint16(setup[2:], tt.flags)
			writeSmbPacket(t, server, smbResponse(request, 0, setup))

			request, readErr := readSmbPacket(server)
			require.NoError(t, server.Close())
			select {
			case err := <-result:
				require.ErrorContains(t, err, "creating SMB session")
				require.ErrorContains(t, err, "guest or anonymous SMB session cannot be signed; use credentials for an authenticated account")
				require.ErrorIs(t, readErr, io.EOF)
				require.Empty(t, request, "must reject the session before TREE_CONNECT")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func readSmbPacket(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	packet := make([]byte, binary.BigEndian.Uint32(header[:]))
	_, err := io.ReadFull(conn, packet)
	return packet, err
}

func smbResponse(request []byte, status uint32, body []byte) []byte {
	packet := make([]byte, 64+len(body))
	copy(packet, "\xfeSMB")
	binary.LittleEndian.PutUint16(packet[4:], 64)
	binary.LittleEndian.PutUint32(packet[8:], status)
	copy(packet[12:14], request[12:14])
	binary.LittleEndian.PutUint16(packet[14:], 1)
	binary.LittleEndian.PutUint32(packet[16:], 1) // Server response, unsigned.
	copy(packet[24:32], request[24:32])
	if binary.LittleEndian.Uint16(request[12:]) != 0 {
		binary.LittleEndian.PutUint64(packet[40:], 1)
	}
	copy(packet[64:], body)
	return packet
}

func writeSmbPacket(t *testing.T, conn net.Conn, packet []byte) {
	t.Helper()
	_, err := conn.Write(smbFrame(packet))
	require.NoError(t, err)
}

func smbFrame(packet []byte) []byte {
	frame := make([]byte, 4+len(packet))
	binary.BigEndian.PutUint32(frame, uint32(len(packet)))
	copy(frame[4:], packet)
	return frame
}
