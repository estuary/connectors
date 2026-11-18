package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf16"

	"github.com/stretchr/testify/require"
)

func TestConnectionRejectsUnsignedSessions(t *testing.T) {
	for _, tt := range []struct {
		name      string
		flags     uint16
		protocol  byte
		tunnel    bool
		decoy     bool
		transform bool
	}{
		{"guest", 0x1, 0xfe, false, false, false},
		{"anonymous", 0x2, 0xfe, false, false, false},
		{"guest and anonymous", 0x3, 0xfe, false, false, false},
		{"guest with invalid protocol", 0x1, 0xff, false, false, false},
		{"encrypted guest after decoy success", 0x1, 0xfd, false, true, false},
		{"non-guest unsigned reply", 0, 0xfe, false, false, false},
		{"non-guest encrypted reply on SMB 2.1", 0, 0xfe, false, false, true},
		{"encrypt data on SMB 2.1", 0x4, 0xfe, false, false, false},
		{"tunneled non-guest unsigned reply", 0, 0xfe, true, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			var previousDial = smbDialContext
			smbDialContext = func(context.Context, string, string) (net.Conn, error) { return client, nil }
			defer func() { smbDialContext = previousDial }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var cfg = config{
				Host: "smb.example", Port: 445,
				Share: "data", Username: "test", Password: "test",
			}
			var address = "smb.example:445"
			if tt.tunnel {
				cfg.NetworkTunnel = testTunnelConfig(t).NetworkTunnel
				address = "127.0.0.1:9445"
			}
			var result = make(chan error, 1)
			go func() {
				conn, err := newSmbConnection(ctx, cfg, address)
				if conn != nil {
					conn.Close()
				}
				result <- err
			}()
			require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))

			request, err := readSmbPacket(server)
			require.NoError(t, err)
			require.Equal(t, uint16(0), binary.LittleEndian.Uint16(request[12:]))
			var negotiate = make([]byte, 64)
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
			var challenge = make([]byte, 52)
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
			var setup = make([]byte, 8+len(token))
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
			if tt.decoy {
				// go-smb2 drops a success response to a MessageId it never sent.
				var decoy = smbResponse(request, 0, setup)
				binary.LittleEndian.PutUint64(decoy[24:], 999)
				writeSmbPacket(t, server, decoy)
			}
			binary.LittleEndian.PutUint16(setup[2:], tt.flags)
			var final = smbResponse(request, 0, setup)
			final[0] = tt.protocol
			if tt.decoy {
				// The client already rejected the decoy and may have closed the connection.
				server.Write(smbFrame(final))
			} else {
				writeSmbPacket(t, server, final)
			}

			request, err = readSmbPacket(server)
			if err == nil {
				require.Equal(t, uint16(3), binary.LittleEndian.Uint16(request[12:]))
				if tt.flags == 0 {
					require.NotZero(t, binary.LittleEndian.Uint32(request[16:])&0x8, "TREE_CONNECT must be signed")
				}
				// Name the configured server, not the dialed address or tunnel listener.
				var offset = binary.LittleEndian.Uint16(request[68:])
				var length = binary.LittleEndian.Uint16(request[70:])
				var name = make([]uint16, length/2)
				require.NoError(t, binary.Read(bytes.NewReader(request[offset:offset+length]), binary.LittleEndian, name))
				require.Equal(t, `\\smb.example\data`, string(utf16.Decode(name)))
				if tt.transform {
					// SMB 2.1 has no encryption, so go-smb2 has no key to decrypt this.
					var transform = make([]byte, 68)
					copy(transform, "\xfdSMB")
					binary.LittleEndian.PutUint32(transform[36:], 16)
					binary.LittleEndian.PutUint16(transform[42:], 1) // Encrypted.
					binary.LittleEndian.PutUint64(transform[44:], 1)
					writeSmbPacket(t, server, transform)
				} else {
					var tree = make([]byte, 16)
					binary.LittleEndian.PutUint16(tree, 16)
					tree[2] = 1 // Disk share.
					writeSmbPacket(t, server, smbResponse(request, 0, tree))
				}
			} else {
				require.ErrorIs(t, err, io.EOF)
			}
			require.NoError(t, server.Close())
			select {
			case err := <-result:
				if tt.decoy {
					require.ErrorContains(t, err, "unexpected SMB2 response")
					require.Empty(t, request, "must reject the session before TREE_CONNECT")
				} else if tt.flags&0x4 != 0 {
					require.ErrorContains(t, err, "SMB server requires encryption but no SMB3 cipher was negotiated")
					require.Empty(t, request, "must reject the session before TREE_CONNECT")
				} else if tt.protocol != 0xfe {
					require.ErrorContains(t, err, "invalid SMB2 response header")
					require.Empty(t, request, "must reject the session before TREE_CONNECT")
				} else if tt.transform {
					require.ErrorContains(t, err, "invalid SMB2 response header")
				} else if tt.flags == 0 {
					require.ErrorContains(t, err, "signing required")
				} else {
					require.ErrorContains(t, err, "guest or anonymous SMB session cannot be signed")
					require.Empty(t, request, "must reject the session before TREE_CONNECT")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestConnectionAcceptsUnsignedInterimResponse(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	var previousDial = smbDialContext
	smbDialContext = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	defer func() { smbDialContext = previousDial }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var cfg = config{
		Host: "smb.example", Port: 445,
		Share: "data", Username: "test", Password: "test",
	}
	var result = make(chan error, 1)
	go func() {
		conn, err := newSmbConnection(ctx, cfg, "smb.example:445")
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		info, err := conn.share.WithContext(ctx).Stat("a.csv")
		if err == nil && info.Size() != 5 {
			err = io.ErrUnexpectedEOF
		}
		result <- err
	}()
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))

	request, err := readSmbPacket(server)
	require.NoError(t, err)
	var negotiate = make([]byte, 64)
	binary.LittleEndian.PutUint16(negotiate, 65)
	binary.LittleEndian.PutUint16(negotiate[2:], 1) // Signing enabled.
	binary.LittleEndian.PutUint16(negotiate[4:], 0x210)
	for _, offset := range []int{28, 32, 36} {
		binary.LittleEndian.PutUint32(negotiate[offset:], 65536)
	}
	writeSmbPacket(t, server, smbResponse(request, 0, negotiate))

	request, err = readSmbPacket(server)
	require.NoError(t, err)
	var challenge = make([]byte, 52)
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
	var setup = make([]byte, 8+len(token))
	binary.LittleEndian.PutUint16(setup, 9)
	binary.LittleEndian.PutUint16(setup[4:], 72)
	binary.LittleEndian.PutUint16(setup[6:], uint16(len(token)))
	copy(setup[8:], token)
	writeSmbPacket(t, server, smbResponse(request, 0xc0000016, setup))

	// Derive the NTLMv2 session key, which SMB 2.1 uses to sign, from the
	// client's AUTHENTICATE message.
	request, err = readSmbPacket(server)
	require.NoError(t, err)
	var auth = request[bytes.Index(request, []byte("NTLMSSP\x00\x03")):]
	var ntResponse = auth[binary.LittleEndian.Uint32(auth[24:]):]
	ntHash, err := hex.DecodeString("0cb6948805f797bf2a82807973b89537") // MD4 of UTF-16 "test".
	require.NoError(t, err)
	var mac = hmac.New(md5.New, ntHash)
	mac.Write([]byte("T\x00E\x00S\x00T\x00")) // Upper-case user name, empty domain.
	mac = hmac.New(md5.New, mac.Sum(nil))
	mac.Write(ntResponse[:16])
	var sessionKey = mac.Sum(nil)
	var sign = func(packet []byte) []byte {
		binary.LittleEndian.PutUint32(packet[16:], binary.LittleEndian.Uint32(packet[16:])|0x8)
		var mac = hmac.New(sha256.New, sessionKey)
		mac.Write(packet)
		copy(packet[48:64], mac.Sum(nil))
		return packet
	}
	setup = make([]byte, 8)
	binary.LittleEndian.PutUint16(setup, 9)
	writeSmbPacket(t, server, smbResponse(request, 0, setup))

	request, err = readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, uint16(3), binary.LittleEndian.Uint16(request[12:]))
	var tree = make([]byte, 16)
	binary.LittleEndian.PutUint16(tree, 16)
	tree[2] = 1 // Disk share.
	writeSmbPacket(t, server, sign(smbResponse(request, 0, tree)))

	// The server answers CREATE with an unsigned interim response, as Windows does
	// while it waits for another client's lease break, and then the signed result.
	request, err = readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, uint16(5), binary.LittleEndian.Uint16(request[12:]))
	var async = func(packet []byte) []byte {
		binary.LittleEndian.PutUint32(packet[16:], binary.LittleEndian.Uint32(packet[16:])|0x2)
		binary.LittleEndian.PutUint64(packet[32:], 9) // AsyncId
		return packet
	}
	var interim = make([]byte, 9)
	binary.LittleEndian.PutUint16(interim, 9)
	writeSmbPacket(t, server, async(smbResponse(request, 0x103, interim)))
	var create = make([]byte, 88)
	binary.LittleEndian.PutUint16(create, 89)
	binary.LittleEndian.PutUint64(create[48:], 5)    // EndOfFile
	binary.LittleEndian.PutUint32(create[56:], 0x80) // FILE_ATTRIBUTE_NORMAL
	writeSmbPacket(t, server, sign(async(smbResponse(request, 0, create))))

	request, err = readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, uint16(6), binary.LittleEndian.Uint16(request[12:]))
	var closed = make([]byte, 60)
	binary.LittleEndian.PutUint16(closed, 60)
	writeSmbPacket(t, server, sign(smbResponse(request, 0, closed)))

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestSigningConnInterimResponses(t *testing.T) {
	var response = func(status, flags uint32, next uint32) []byte {
		var packet = make([]byte, 64)
		copy(packet, "\xfeSMB")
		binary.LittleEndian.PutUint32(packet[8:], status)
		binary.LittleEndian.PutUint32(packet[16:], flags)
		binary.LittleEndian.PutUint32(packet[20:], next)
		binary.LittleEndian.PutUint64(packet[24:], 5)
		binary.LittleEndian.PutUint64(packet[40:], 7)
		return packet
	}
	for _, tt := range []struct {
		name    string
		frame   []byte
		cleared []bool // Whether each response's SessionId was cleared.
		prefix  bool
	}{
		{"unsigned interim", response(0x103, 0x3, 0), []bool{true}, true},
		{"signed interim", response(0x103, 0xb, 0), []bool{false}, false},
		{"synchronous pending", response(0x103, 0x1, 0), []bool{false}, false},
		{"unsigned final", response(0, 0x3, 0), []bool{false}, false},
		{"compound", append(response(0, 0x9, 64), response(0x103, 0x7, 0)...), []bool{false, true}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var conn = &smbSigningConn{Conn: &smbReadConn{reader: bytes.NewReader(smbFrame(tt.frame))}, authenticated: true}
			packet, err := readSmbPacket(conn)
			require.NoError(t, err)
			if tt.prefix {
				require.Equal(t, uint64(0xffffffffffffffff), binary.LittleEndian.Uint64(packet[24:]))
				require.Equal(t, uint32(64), binary.LittleEndian.Uint32(packet[20:]))
				require.Equal(t, uint64(7), binary.LittleEndian.Uint64(packet[40:]))
				packet = packet[64:]
			}
			require.Len(t, packet, len(tt.frame))
			for i, cleared := range tt.cleared {
				var sessionID = binary.LittleEndian.Uint64(packet[64*i+40:])
				require.Equal(t, cleared, sessionID == 0)
			}
		})
	}
}

func readSmbPacket(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	var packet = make([]byte, binary.BigEndian.Uint32(header[:]))
	_, err := io.ReadFull(conn, packet)
	return packet, err
}

func smbResponse(request []byte, status uint32, body []byte) []byte {
	var packet = make([]byte, 64+len(body))
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
	var frame = make([]byte, 4+len(packet))
	binary.BigEndian.PutUint32(frame, uint32(len(packet)))
	copy(frame[4:], packet)
	return frame
}

func TestSigningConnFraming(t *testing.T) {
	// The client sends NEGOTIATE as MessageId 0 and SESSION_SETUP as MessageId 1.
	var request = make([]byte, 64)
	copy(request, "\xfeSMB")
	var negotiateRequest = append([]byte(nil), request...)
	binary.LittleEndian.PutUint16(request[12:], 1)
	binary.LittleEndian.PutUint64(request[24:], 1)
	var setupRequest = append([]byte(nil), request...)
	var negotiate = func(dialect, cipher uint16) []byte {
		var body = make([]byte, 80)
		binary.LittleEndian.PutUint16(body, 65)
		binary.LittleEndian.PutUint16(body[4:], dialect)
		if cipher != 0 {
			binary.LittleEndian.PutUint16(body[6:], 1)    // One negotiate context,
			binary.LittleEndian.PutUint32(body[60:], 128) // after the fixed body.
			binary.LittleEndian.PutUint16(body[64:], 2)   // SMB2_ENCRYPTION_CAPABILITIES
			binary.LittleEndian.PutUint16(body[66:], 4)
			binary.LittleEndian.PutUint16(body[72:], 1)
			binary.LittleEndian.PutUint16(body[74:], cipher)
		}
		return smbResponse(negotiateRequest, 0, body)
	}
	var decoyNegotiate = negotiate(0x300, 0)
	binary.LittleEndian.PutUint64(decoyNegotiate[24:], 999)
	var body = make([]byte, 8)
	binary.LittleEndian.PutUint16(body, 9)
	var authenticated = smbResponse(request, 0, body)
	var decoy = append([]byte(nil), authenticated...)
	binary.LittleEndian.PutUint64(decoy[24:], 999)
	body[2] = 1
	var guest = smbResponse(request, 0, body)
	body[2] = 2
	var anonymous = smbResponse(request, 0, body)
	body[2] = 4
	var encryptedSession = smbResponse(request, 0, body)
	var compound = append(append([]byte(nil), authenticated...), guest...)
	binary.LittleEndian.PutUint32(compound[20:], uint32(len(authenticated)))
	var invalidCompound = append([]byte(nil), compound...)
	binary.LittleEndian.PutUint32(invalidCompound[20:], ^uint32(0))
	var overlappingCompound = append([]byte(nil), compound...)
	binary.LittleEndian.PutUint32(overlappingCompound[20:], 32)
	var badHeader = smbFrame(authenticated)
	badHeader[0] = 1
	var invalidProtocol = append([]byte(nil), guest...)
	invalidProtocol[0] = 0xff
	var compressed = append([]byte(nil), guest...)
	compressed[0] = 0xfc
	var encrypted = append([]byte("\xfdSMB"), guest...)
	binary.LittleEndian.PutUint16(request[12:], 8) // File data may contain arbitrary bytes.
	binary.LittleEndian.PutUint64(request[24:], 2)
	var data = smbResponse(request, 0, guest)
	var frames = func(packets ...[]byte) []byte {
		var wire []byte
		for _, packet := range packets {
			wire = append(wire, smbFrame(packet)...)
		}
		return wire
	}

	for _, tt := range []struct {
		name     string
		accepted []byte // Frames delivered before the rejected one.
		wire     []byte
		wantErr  string
	}{
		{"coalesced frames", nil, frames(negotiate(0x300, 0), authenticated, data, encryptedSession, encrypted), ""},
		{"encrypted with SMB 3.1.1 cipher", nil, frames(negotiate(0x311, 2), authenticated, encrypted), ""},
		{"guest", nil, smbFrame(guest), "guest or anonymous SMB session cannot be signed"},
		{"anonymous", nil, smbFrame(anonymous), "guest or anonymous SMB session cannot be signed"},
		{"compound guest", nil, smbFrame(compound), "guest or anonymous SMB session cannot be signed"},
		{"invalid compound offset", nil, smbFrame(invalidCompound), "invalid SMB2 compound response offset"},
		{"overlapping compound", nil, smbFrame(overlappingCompound), "invalid SMB2 compound response offset"},
		{"invalid transport", nil, badHeader, "invalid SMB direct-TCP frame"},
		{"invalid protocol identifier", nil, smbFrame(invalidProtocol), "invalid SMB2 response header"},
		{"compression transform", nil, smbFrame(compressed), "invalid SMB2 response header"},
		{"encrypted before authentication", frames(negotiate(0x300, 0)), smbFrame(encrypted), "invalid SMB2 response header"},
		{"encrypted after decoy session setup", frames(negotiate(0x300, 0)), frames(decoy, encrypted), "unexpected SMB2 response"},
		{"encrypted after decoy negotiate", frames(negotiate(0x210, 0)), frames(decoyNegotiate, authenticated, encrypted), "unexpected SMB2 response"},
		{"encryption required on SMB 2.1", frames(negotiate(0x210, 0)), smbFrame(encryptedSession), "SMB server requires encryption but no SMB3 cipher was negotiated"},
		{"encryption required on SMB 3.1.1 without cipher", frames(negotiate(0x311, 0)), smbFrame(encryptedSession), "SMB server requires encryption but no SMB3 cipher was negotiated"},
		{"encrypted with SMB 2.1", frames(negotiate(0x210, 0), authenticated), smbFrame(encrypted), "invalid SMB2 response header"},
		{"encrypted with SMB 3.1.1 without cipher", frames(negotiate(0x311, 0), authenticated), smbFrame(encrypted), "invalid SMB2 response header"},
		{"empty header", nil, smbFrame(nil), "invalid SMB2 response header"},
		{"short protocol identifier", nil, smbFrame(authenticated[:3]), "invalid SMB2 response header"},
		{"short header", nil, smbFrame(authenticated[:32]), "invalid SMB2 response header"},
		{"short setup", nil, smbFrame(authenticated[:67]), "invalid SMB2 session setup response"},
		{"truncated transport", nil, smbFrame(authenticated)[:2], "unexpected EOF"},
		{"truncated payload", nil, smbFrame(authenticated)[:70], "unexpected EOF"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, fragmented := range []bool{false, true} {
				var reader io.Reader = bytes.NewReader(append(append([]byte(nil), tt.accepted...), tt.wire...))
				if fragmented {
					reader = iotest.OneByteReader(reader)
				}
				var conn = &smbSigningConn{Conn: &smbReadConn{reader: reader}}
				var requests = frames(negotiateRequest, setupRequest)
				for len(requests) != 0 {
					var n = len(requests)
					if fragmented {
						n = 1
					}
					_, err := conn.Write(requests[:n])
					require.NoError(t, err)
					requests = requests[n:]
				}
				data, err := io.ReadAll(iotest.OneByteReader(conn))
				if tt.wantErr == "" {
					require.NoError(t, err)
					require.Equal(t, tt.wire, data)
				} else {
					require.ErrorContains(t, err, tt.wantErr)
					require.Equal(t, string(tt.accepted), string(data), "must not deliver a rejected frame")
					_, repeatedErr := conn.Read(make([]byte, 1))
					require.Equal(t, err, repeatedErr)
				}
			}
		})
	}
}

func TestSigningConnResponseBeforeRequest(t *testing.T) {
	var request = make([]byte, 64)
	copy(request, "\xfeSMB")
	var negotiateRequest = append([]byte(nil), request...)
	binary.LittleEndian.PutUint16(request[12:], 1)
	binary.LittleEndian.PutUint64(request[24:], 1)
	var setupRequest = append([]byte(nil), request...)
	var body = make([]byte, 80)
	binary.LittleEndian.PutUint16(body, 65)
	binary.LittleEndian.PutUint16(body[4:], 0x300)
	var negotiate = smbResponse(negotiateRequest, 0, body)
	body = make([]byte, 8)
	binary.LittleEndian.PutUint16(body, 9)
	var challenge = smbResponse(setupRequest, 0xc0000016, body)
	var pending = smbResponse(setupRequest, 0x103, body)
	var success = smbResponse(setupRequest, 0, body)
	body[2] = 1
	var guest = append([]byte("\xfdSMB"), smbResponse(setupRequest, 0, body)...)

	for _, tt := range []struct {
		name  string
		steps [][]byte // Requests to write, or responses to read, in order.
		write []bool
	}{
		// go-smb2 registers a request before writing it, so it can accept a
		// response that arrives in between.
		{"session setup", [][]byte{negotiateRequest, negotiate, challenge, setupRequest, success, guest}, []bool{true, false, false, true, false, false}},
		{"interim session setup", [][]byte{negotiateRequest, negotiate, pending, setupRequest, success, guest}, []bool{true, false, false, true, false, false}},
		{"negotiate", [][]byte{negotiate, negotiateRequest, negotiate}, []bool{false, true, false}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var reader, writer = io.Pipe()
			var conn = &smbSigningConn{Conn: &smbReadConn{reader: reader}}
			var err error
			for i, step := range tt.steps {
				if tt.write[i] {
					_, err = conn.Write(smbFrame(step))
					require.NoError(t, err)
					continue
				}
				go writer.Write(smbFrame(step))
				if _, err = readSmbPacket(conn); err != nil {
					break
				}
			}
			require.ErrorContains(t, err, "unexpected SMB2 response")
			writer.Close()
		})
	}
}

type smbReadConn struct {
	net.Conn
	reader io.Reader
}

func (c *smbReadConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *smbReadConn) Write(p []byte) (int, error) {
	return len(p), nil
}
