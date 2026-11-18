package main

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

// go-smb2 v1.1.0 only checks guest/null flags on the first SESSION_SETUP response.
// Inspect complete direct-TCP frames before delivering the final response, which
// would otherwise disable signing even with RequireMessageSigning enabled.
type smbSigningConn struct {
	net.Conn
	buffer  []byte
	pending []byte
	err     error
	// Set only by responses to requests the client sent, which go-smb2 accepts.
	encryption    bool
	authenticated bool

	mu       sync.Mutex
	requests map[uint64]uint16 // Unanswered NEGOTIATE and SESSION_SETUP requests by MessageId.
	written  []byte            // Start of the outgoing direct-TCP frame.
	skip     int               // Remaining bytes of the outgoing frame.
}

func (c *smbSigningConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.recordRequests(p)
	c.mu.Unlock()
	return c.Conn.Write(p)
}

// recordRequests tracks outgoing frames, which go-smb2 writes as a length prefix
// followed by the packet.
func (c *smbSigningConn) recordRequests(p []byte) {
	for len(p) != 0 {
		if c.skip != 0 {
			var n = min(c.skip, len(p))
			c.skip, p = c.skip-n, p[n:]
			continue
		}
		var n = min(c.headerSize()-len(c.written), len(p))
		c.written, p = append(c.written, p[:n]...), p[n:]
		if len(c.written) < c.headerSize() {
			continue
		}
		// Command 0 is NEGOTIATE and 1 is SESSION_SETUP.
		var packet = c.written[4:]
		if len(packet) == 32 && string(packet[:4]) == "\xfeSMB" && binary.LittleEndian.Uint16(packet[12:]) <= 1 {
			if c.requests == nil {
				c.requests = make(map[uint64]uint16)
			}
			c.requests[binary.LittleEndian.Uint64(packet[24:])] = binary.LittleEndian.Uint16(packet[12:])
		}
		c.skip = 4 + int(binary.BigEndian.Uint32(c.written)) - len(c.written)
		c.written = c.written[:0]
	}
}

// headerSize covers the frame length and the SMB2 header through its MessageId.
func (c *smbSigningConn) headerSize() int {
	if len(c.written) < 4 {
		return 4
	}
	return 4 + min(int(binary.BigEndian.Uint32(c.written)), 32)
}

// answers reports whether a response matches an unanswered request for the same
// command. go-smb2 drops responses with unknown MessageIds and forgets answered
// ones, but keeps a request after an interim response.
func (c *smbSigningConn) answers(command uint16, messageID uint64, interim bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	var requested, ok = c.requests[messageID]
	if !interim {
		delete(c.requests, messageID)
	}
	return ok && requested == command
}

func (c *smbSigningConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if c.err != nil {
		return 0, c.err
	}
	if len(c.pending) == 0 {
		if c.err = c.readFrame(); c.err != nil {
			return 0, c.err
		}
		c.pending = c.buffer
	}
	var n = copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *smbSigningConn) readFrame() error {
	var header [4]byte
	if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
		return err
	}
	if header[0] != 0 {
		return errors.New("invalid SMB direct-TCP frame")
	}
	var size = 4 + int(binary.BigEndian.Uint32(header[:]))
	if cap(c.buffer) < size {
		c.buffer = make([]byte, size)
	} else {
		c.buffer = c.buffer[:size]
	}
	copy(c.buffer, header[:])
	if _, err := io.ReadFull(c.Conn, c.buffer[4:]); err != nil {
		return err
	}
	if err := c.checkSmbSessionFlags(c.buffer[4:]); err != nil {
		return err
	}
	c.passInterimResponses()
	return nil
}

// passInterimResponses lets go-smb2 v1.1.0 accept unsigned interim responses.
// Servers do not sign interim async STATUS_PENDING responses (MS-SMB2 3.3.4.1.1),
// but go-smb2 rejects any unsigned response on a signed session and fails the
// request. It skips the signature check for an unknown SessionId, yet drops a
// frame whose first response has one. So clear the SessionId of each unsigned
// interim response, and put a placeholder header carrying the SessionId in front
// of a frame that starts with one. go-smb2 ignores the placeholder, whose
// MessageId it never sent, and still verifies the final response.
func (c *smbSigningConn) passInterimResponses() {
	var packet = c.buffer[4:]
	if !c.authenticated || string(packet[:4]) != "\xfeSMB" {
		return
	}
	var placeholder []byte
	for first := true; ; first = false {
		var flags = binary.LittleEndian.Uint32(packet[16:])
		// Status 0x103 is STATUS_PENDING. Flag 0x2 is ASYNC_COMMAND and 0x8 is SIGNED.
		if binary.LittleEndian.Uint32(packet[8:]) == 0x103 && flags&0xa == 0x2 {
			if first {
				placeholder = make([]byte, 64)
				copy(placeholder, "\xfeSMB")
				binary.LittleEndian.PutUint16(placeholder[4:], 64)
				binary.LittleEndian.PutUint32(placeholder[16:], 0x1) // Server response.
				binary.LittleEndian.PutUint32(placeholder[20:], 64)  // Next response.
				binary.LittleEndian.PutUint64(placeholder[24:], 0xffffffffffffffff)
				copy(placeholder[40:48], packet[40:48])
			}
			clear(packet[40:48])
		}
		var next = int(binary.LittleEndian.Uint32(packet[20:]))
		if next == 0 {
			break
		}
		packet = packet[next:]
	}
	if placeholder != nil {
		var frame = make([]byte, 4, len(c.buffer)+len(placeholder))
		binary.BigEndian.PutUint32(frame, uint32(len(c.buffer)-4+len(placeholder)))
		c.buffer = append(append(frame, placeholder...), c.buffer[4:]...)
	}
}

func (c *smbSigningConn) checkSmbSessionFlags(packet []byte) error {
	if len(packet) >= 4 && string(packet[:4]) == "\xfdSMB" && c.authenticated && c.encryption {
		// Encrypted SMB3 frames are authenticated and decoded by go-smb2, which has
		// a decryption key only once an authenticated session negotiated encryption.
		return nil
	}
	for {
		if len(packet) < 64 || string(packet[:4]) != "\xfeSMB" {
			return errors.New("invalid SMB2 response header")
		}
		var response = packet
		var next = int(binary.LittleEndian.Uint32(packet[20:]))
		if next != 0 {
			if next < 64 || next > len(packet)-64 {
				return errors.New("invalid SMB2 compound response offset")
			}
			response = packet[:next]
		}
		var command = binary.LittleEndian.Uint16(response[12:])
		var status = binary.LittleEndian.Uint32(response[8:])
		// Command 1 is SESSION_SETUP; status 0 is STATUS_SUCCESS.
		if command == 1 && status == 0 {
			if len(response) < 72 {
				return errors.New("invalid SMB2 session setup response")
			}
			var flags = binary.LittleEndian.Uint16(response[66:])
			if flags&0x3 != 0 { // IS_GUEST | IS_NULL
				return errors.New("guest or anonymous SMB session cannot be signed; use credentials for an authenticated account")
			}
			// go-smb2 would encrypt later requests with ENCRYPT_DATA but has no cipher.
			if flags&0x4 != 0 && !c.encryption {
				return errors.New("SMB server requires encryption but no SMB3 cipher was negotiated")
			}
		}
		// Status 0x103 is STATUS_PENDING, which go-smb2 follows with the final response.
		var matched = c.answers(command, binary.LittleEndian.Uint64(response[24:]), status == 0x103)
		// go-smb2 registers a request before writing it, so it may accept a response
		// read before the request was recorded here. Fail rather than lose track.
		if !matched && !c.authenticated {
			return errors.New("unexpected SMB2 response before the session was authenticated")
		}
		if matched && status == 0 {
			if command == 0 {
				c.encryption = negotiatesEncryption(response[64:])
			} else {
				c.authenticated = true
			}
		}
		if next == 0 {
			return nil
		}
		packet = packet[next:]
	}
}

// negotiatesEncryption reports whether go-smb2 derives a decryption key after this
// NEGOTIATE response: for SMB 3.0 and 3.0.2, and for SMB 3.1.1 only with a cipher.
func negotiatesEncryption(body []byte) bool {
	if len(body) < 64 {
		return false
	}
	switch binary.LittleEndian.Uint16(body[4:]) {
	case 0x300, 0x302:
		return true
	case 0x311:
	default:
		return false
	}
	var offset = int(binary.LittleEndian.Uint32(body[60:])) - 64
	if offset < 0 || offset > len(body) {
		return false
	}
	var contexts, cipher = body[offset:], false
	for count := binary.LittleEndian.Uint16(body[6:]); count > 0; count-- {
		if len(contexts) < 8 || len(contexts) < 8+int(binary.LittleEndian.Uint16(contexts[2:])) {
			return false
		}
		var size = 8 + int(binary.LittleEndian.Uint16(contexts[2:]))
		// Type 2 is SMB2_ENCRYPTION_CAPABILITIES. go-smb2 requires exactly one
		// cipher, either AES-128-CCM (1) or AES-128-GCM (2).
		if binary.LittleEndian.Uint16(contexts) == 2 {
			var data = contexts[8:size]
			cipher = len(data) >= 4 && binary.LittleEndian.Uint16(data) == 1 &&
				(binary.LittleEndian.Uint16(data[2:]) == 1 || binary.LittleEndian.Uint16(data[2:]) == 2)
		}
		contexts = contexts[min((size+7)&^7, len(contexts)):]
	}
	return cipher
}
