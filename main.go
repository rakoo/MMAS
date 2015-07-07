package main

import (
	"bytes"
	"crypto/sha1"
	"database/sql"
	"io"
	"log"
	"net/http"
	"time"

	"camlistore.org/pkg/rollsum"
	"github.com/elazarl/goproxy"

	_ "github.com/mattn/go-sqlite3"
)

const (
	sqlUpSert = `
	INSERT OR REPLACE INTO chunks VALUES (
		?,
		?,
		COALESCE(1 + (SELECT count FROM chunks WHERE hash = ?), 1)
	);`
)

func main() {
	proxy := goproxy.NewProxyHttpServer()
	proxy.Verbose = true

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE chunks (
		content BLOB,
		hash BLOB UNIQUE ON CONFLICT REPLACE,
		count INTEGER
	);`)
	if err != nil {
		log.Fatal(err)
	}

	var preums []byte

	proxy.OnResponse().Do(goproxy.HandleBytes(func(body []byte, ctx *goproxy.ProxyCtx) []byte {
		start := time.Now()

		rs := rollsum.New()
		rd := bytes.NewReader(body)
		buf := make([]byte, 0)

		tx, err := db.Begin()
		if err != nil {
			log.Println(err)
			return body
		}

		stmt, err := tx.Prepare(sqlUpSert)
		if err != nil {
			log.Println(err)
			return body
		}

		for {
			b, err := rd.ReadByte()
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Println(err)
					return body
				}
			}
			rs.Roll(b)
			buf = append(buf, b)
			if rs.OnSplitWithBits(5) {
				h := sha1.Sum(buf)
				buf = buf[:0]
				_, err := stmt.Exec(buf, h[:], h[:])
				if err != nil {
					log.Println(err)
					return body
				}
			}
		}

		tx.Commit()

		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM chunks`).Scan(&count)
		if err != nil {
			log.Println(err)
			return body
		}

		var dups int
		err = db.QueryRow(`SELECT COUNT(*) FROM chunks WHERE count > 1`).Scan(&dups)
		if err != nil {
			log.Println(err)
			return body
		}
		log.Printf("%d dups / %d chunks\n", dups, count)
		var preumsCandidate []byte
		err = db.QueryRow(`SELECT hash FROM chunks ORDER BY count, hash DESC LIMIT 1`).Scan(&preumsCandidate)
		if err != nil {
			log.Println(err)
			return body
		}

		if bytes.Compare(preums, preumsCandidate) != 0 {
			preums = preumsCandidate
			log.Println("Changed preums")
		}
		log.Printf("Took %v ms\n", time.Since(start).Seconds()*1000)
		return body
	}))

	log.Fatal(http.ListenAndServe(":8080", proxy))
}
