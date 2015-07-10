package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"log"
	"os/exec"
	"path"
	"time"
)

func (bh *bodyHandler) makeDiff(body []byte) (newBody []byte, err error) {
	startDelta := time.Now()

	fullDictHash := path.Base(bh.dictFileName)
	rawServerId, err := hex.DecodeString(fullDictHash)
	serverId := base64.URLEncoding.EncodeToString(rawServerId[6:12])
	if err != nil {
		return body, err
	}

	var out bytes.Buffer
	if _, err = out.WriteString(serverId); err != nil {
		return body, err
	}
	if err := out.WriteByte(byte(0)); err != nil {
		return body, err
	}

	cmd := exec.Command("vcdiff", "delta", "-dictionary", bh.dictFileName, "-interleaved", "-checksum", "-stats")
	cmd.Stdin = bytes.NewReader(body)

	cmd.Stdout = &out

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return body, err
	}

	log.Printf("[VCDIFF-DELTA] %s\n", stderr.String())

	log.Printf("Generated delta in %f msecs\n", time.Since(startDelta).Seconds()*1000)
	return out.Bytes(), nil
}
