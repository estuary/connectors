package testutil

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// sopsDecryptions memoizes decryptions by input. Every decrypt is a KMS round
// trip, and a suite decrypts the same config for most of its subtests.
var sopsDecryptions sync.Map // string -> *sopsDecryption

type sopsDecryption struct {
	once      sync.Once
	plaintext []byte
	err       error
}

// DecryptSops decrypts a sops document of inputType ("json" or "yaml") to JSON
// and strips the _sops suffixes from its keys.
func DecryptSops(ciphertext []byte, inputType string) ([]byte, error) {
	v, _ := sopsDecryptions.LoadOrStore(inputType+"\x00"+string(ciphertext), &sopsDecryption{})
	d := v.(*sopsDecryption)
	d.once.Do(func() { d.plaintext, d.err = decryptSopsWithRetry(ciphertext, inputType) })
	return bytes.Clone(d.plaintext), d.err
}

// decryptSopsWithRetry retries because sops fails transiently in CI, exiting
// 128 when it can't retrieve the data key from KMS.
func decryptSopsWithRetry(ciphertext []byte, inputType string) ([]byte, error) {
	const attempts = 4
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(1<<(attempt-2)) * time.Second)
		}
		var plaintext []byte
		if plaintext, err = runSopsDecrypt(ciphertext, inputType); err == nil {
			return stripSopsSuffixes(plaintext)
		}
	}
	return nil, fmt.Errorf("sops decrypt failed after %d attempts: %w", attempts, err)
}

func runSopsDecrypt(ciphertext []byte, inputType string) ([]byte, error) {
	cmd := exec.Command("sops", "--decrypt", "--input-type", inputType, "--output-type", "json", "/dev/stdin")
	cmd.Stdin = bytes.NewReader(ciphertext)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func stripSopsSuffixes(plaintext []byte) ([]byte, error) {
	cmd := exec.Command("jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
	cmd.Stdin = bytes.NewReader(plaintext)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("stripping _sops suffixes: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}
