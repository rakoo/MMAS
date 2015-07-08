package main

import (
	"bytes"
	"crypto/sha1"
	"database/sql"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
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

const (
	dictPath   = "/var/tmp/mmas-dict"
	chunksPath = "/var/tmp/mmas-chunks"
)

type bodyHandler struct {
	db     *sql.DB
	preums []byte
}

func (bh *bodyHandler) handler() func(body []byte, ctx *goproxy.ProxyCtx) []byte {
	return func(body []byte, ctx *goproxy.ProxyCtx) []byte {
		newBody := body
		var err error

		st, err := os.Stat(dictPath)
		if (err == nil || os.IsExist(err)) && st.Size() > 0 {
			newBody, err = bh.makeDelta(body)
			if err != nil {
				log.Println(err)
				return body
			}
		} else if err != nil && !os.IsNotExist(err) {
			log.Println(err)
			return body
		}

		changedPreums, err := bh.parseResponse(body)
		if err != nil {
			log.Println(err)
			return body
		}

		if changedPreums {
			bh.makeDict()
		}

		return newBody
	}
}

func (bh *bodyHandler) makeDelta(body []byte) (newBody []byte, err error) {
	startDelta := time.Now()

	cmd := exec.Command("vcdiff", "encode", "-dictionary", dictPath, "-interleaved", "-checksum", "-stats")
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

	var count, countbytes int
	err = bh.db.QueryRow(`SELECT COUNT(*), SUM(LENGTH(content)) FROM chunks`).Scan(&count, &countbytes)
	if err != nil {
		return false, err
	}

	var dups, dupsbytes int
	err = bh.db.QueryRow(`SELECT COUNT(*), SUM(LENGTH(content)) FROM chunks WHERE count > 1`).Scan(&dups, &dupsbytes)
	if err != nil {
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

	//log.Printf("%d dups / %d chunks (%d / %d bytes) \n", dups, count, dupsbytes, countbytes)
	log.Printf("Parsed response in %v ms\n", time.Since(startParse).Seconds()*1000)

	return changedPreums, nil
}

func (bh *bodyHandler) makeDict() {
	start := time.Now()
	rows, err := bh.db.Query(`SELECT count, content FROM chunks ORDER BY count, content DESC`)
	if err != nil {
		log.Println(err)
		return
	}

	allReaders := make([]io.Reader, 0)
	for rows.Next() {
		var count int
		var content []byte
		err := rows.Scan(&count, &content)
		if err != nil {
			log.Println(err)
			return
		}
		for i := 0; i < count; i++ {
			allReaders = append(allReaders, bytes.NewReader(content))
		}
	}

	if err := rows.Err(); err != nil {
		log.Println(err)
		return
	}

	cmd := exec.Command("vcdiff", "encode", "-dictionary", "/dev/zero", "-target_matches", "-delta", dictPath)
	cmd.Stderr = os.Stderr
	cmd.Stdin = io.MultiReader(allReaders...)
	if err = cmd.Run(); err != nil {
		log.Println(err)
		return
	}

	st, err := os.Stat(dictPath)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("Generated a %d bytes dict in %f msecs\n", st.Size(), time.Since(start).Seconds()*1000)
}

func main() {
	proxy := goproxy.NewProxyHttpServer()

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

	bh := &bodyHandler{
		db: db,
	}
	proxy.OnResponse().Do(goproxy.HandleBytes(bh.handler()))

	log.Println("Let's go !")
	log.Fatal(http.ListenAndServe(":8080", proxy))
}
