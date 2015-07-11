package main

import (
	"bytes"
	"crypto/sha1"
	"io"
	"log"
	"time"

	"camlistore.org/pkg/rollsum"
)

const (
	sqlUpSert = `
	INSERT OR REPLACE INTO chunks VALUES (
		?,
		?,
		COALESCE(1 + (SELECT count FROM chunks WHERE hash = ?), 1)
	);`
)

func (bh *bodyHandler) parseResponse(body []byte) (changed bool, err error) {
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

	log.Printf("Parsed response in %v ms\n", time.Since(startParse).Seconds()*1000)

	// Heuristic for changed: if the old top chunk is not in the new
	// first 10, consider the current dictionary as old
	top10Rows, err := bh.db.Query(`SELECT hash FROM chunks ORDER BY count, hash DESC LIMIT 10`)
	if err != nil {
		return false, err
	}

	top10 := make([][]byte, 0, 10)
	for top10Rows.Next() {
		var hash []byte
		err := top10Rows.Scan(&hash)
		if err != nil {
			return false, err
		}
		top10 = append(top10, hash)
	}
	if err := top10Rows.Err(); err != nil {
		return false, err
	}

	log.Println(len(bh.topChunk), len(top10))
	if len(bh.topChunk) == 0 && len(top10) > 0 {
		bh.topChunk = top10[0]
		return true, nil
	}

	for _, newInTop := range top10 {
		if bytes.Compare(bh.topChunk, newInTop) == 0 {
			bh.topChunk = newInTop
			return true, nil
		}
	}

	return false, nil
}
