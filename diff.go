package main

import (
	"bytes"
	"log"
	"os/exec"
	"time"
)

func (bh *bodyHandler) makeDiff(body []byte) (newBody []byte, err error) {
	startDelta := time.Now()

	cmd := exec.Command("vcdiff", "encode", "-dictionary", bh.dictFileName, "-interleaved", "-checksum", "-stats")
	cmd.Stdin = bytes.NewReader(body)
	var out bytes.Buffer
	cmd.Stdout = &out

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	log.Printf("[VCDIFF-DELTA] %s\n", stderr.String())

	log.Printf("Generated delta in %f msecs\n", time.Since(startDelta).Seconds()*1000)
	return out.Bytes(), nil
}
