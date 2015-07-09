package main

import (
	"bytes"
	"crypto/sha1"
	"io"
	"log"
	"time"

	"camlistore.org/pkg/rollsum"
)

func (bh *bodyHandler) parseResponse(body []byte) (changedPreums bool, err error) {
	startParse := time.Now()

	rs := rollsum.New()
	rd := bytes.NewReader(body)
	buf := make([]byte, 0)

	tx, err := bh.db.Begin()
	if err != nil {
		return false, err
	}

	stmt, err := tx.Prepare(sqlUpSert)
	if err != nil {
		return false, err
	}

	for {
		b, err := rd.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return false, err
			}
		}
		rs.Roll(b)
		buf = append(buf, b)
		if rs.OnSplitWithBits(5) {
			h := sha1.Sum(buf)
			_, err := stmt.Exec(buf, h[:], h[:])
			if err != nil {
				return false, err
			}
			buf = buf[:0]
		}
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	var preumsCandidate []byte
	err = bh.db.QueryRow(`SELECT hash FROM chunks ORDER BY count, hash DESC LIMIT 1`).Scan(&preumsCandidate)
	if err != nil {
		return false, err
	}

	if bytes.Compare(bh.preums, preumsCandidate) != 0 {
		bh.preums = preumsCandidate
		changedPreums = true
	}

	log.Printf("Parsed response in %v ms\n", time.Since(startParse).Seconds()*1000)

	return changedPreums, nil
}
