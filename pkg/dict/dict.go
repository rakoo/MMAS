package dict

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"sort"

	"camlistore.org/pkg/rollsum"

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

var (
	ErrNoDict = errors.New("No dictionary")
)

type Dict struct {
	db *sql.DB

	sdchDictChunks [][]byte
	sdchFullHash   []byte

	// stats
	totalBytesDup uint64
	totalBytesIn  uint64

	SdchHeader []byte
}

func New() (*Dict, error) {
	db, err := sql.Open("sqlite3", "dict")
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	_, err = db.Exec(
		`
CREATE TABLE IF NOT EXISTS chunks (
		content BLOB,
		hash BLOB UNIQUE ON CONFLICT REPLACE,
		count INTEGER
);`)
	if err != nil {
		return nil, err
	}

	return &Dict{
		db: db,
	}, nil
}

func (d *Dict) UserAgentId() []byte {
	if len(d.sdchFullHash) == 0 {
		return []byte{}
	}
	return based(d.sdchFullHash[6:12])
}

func (d *Dict) ServerId() []byte {
	if len(d.sdchFullHash) == 0 {
		return []byte{}
	}
	return based(d.sdchFullHash[:6])
}

func based(in []byte) []byte {
	dst := make([]byte, base64.URLEncoding.EncodedLen(len(in)))
	base64.URLEncoding.Encode(dst, in)
	return dst
}

func (d *Dict) DictName() string {
	return hex.EncodeToString(d.sdchFullHash)
}

func (d *Dict) Eat(content []byte) (diff []byte, err error) {

	go func() {
		err := d.parse(content)
		if err != nil {
			log.Println("Error parsing:", err)
		}
	}()

	if len(d.SdchHeader) == 0 {
		return nil, ErrNoDict
	}

	dictpath := path.Join("dicts", hex.EncodeToString(d.sdchFullHash))
	var diffBuf bytes.Buffer
	cmd := exec.Command("vcdiff", "delta", "-dictionary", dictpath, "-interleaved", "-stats", "-checksum")
	cmd.Stdin = bytes.NewReader(content)
	cmd.Stdout = &diffBuf
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	diff = diffBuf.Bytes()

	return diff, nil
}

func (d *Dict) parse(content []byte) error {
	rs := rollsum.New()

	var match uint64
	q := content
	buf := make([]byte, 0)
	hashes := make([][]byte, 0)
	offs := make([]int, 0)

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(sqlUpSert)
	if err != nil {
		return err
	}

	off := 0
	for len(q) > 0 {
		b := q[0]
		q = q[1:]

		rs.Roll(b)
		off++
		d.totalBytesIn++

		buf = append(buf, b)
		if rs.OnSplitWithBits(5) {
			h := sha1.Sum(buf)
			offs = append(offs, off)
			hashes = append(hashes, h[:])

			_, err := stmt.Exec(buf, h[:], h[:])
			if err != nil {
				return err
			}
			buf = buf[:0]
		}
	}

	d.totalBytesDup += uint64(match)

	if errStmt := stmt.Close(); errStmt != nil {
		return err
	}

	if errTx := tx.Commit(); errTx != nil {
		return err
	}

	err = d.makeDict()
	if err != nil {
		return err
	}

	return nil
}

func (d *Dict) makeDict() error {
	contents, hashes, change := d.needToUpdate()
	if change {
		log.Println("Changing dict")
		d.sdchDictChunks = hashes

		hash := sha256.New()
		var buf bytes.Buffer
		mw := io.MultiWriter(&buf, hash)
		fmt.Fprint(mw, "Domain: localhost\n")
		fmt.Fprint(mw, "Path: /\n")
		fmt.Fprint(mw, "Format-Version: 1.0\n")
		fmt.Fprint(mw, "Port: 8080\n")
		fmt.Fprint(mw, "Max-Age: 86400\n\n")

		d.SdchHeader = buf.Bytes()

		hash.Write(contents)
		h := hash.Sum(nil)
		d.sdchFullHash = hash.Sum(nil)

		dictpath := path.Join("dicts", hex.EncodeToString(h))
		err := ioutil.WriteFile(dictpath, contents, 0644)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Dict) needToUpdate() (contents []byte, hashes [][]byte, change bool) {
	rows, err := d.db.Query(`SELECT hash, content FROM chunks WHERE COUNT > 1 ORDER BY count, hash DESC`)
	if err != nil {
		log.Println(err)
		return nil, nil, false
	}
	defer rows.Close()

	hashes = make([][]byte, 0)
	contents = make([]byte, 0)
	var hash, content []byte
	for rows.Next() {
		rows.Scan(&hash, &content)
		contents = append(contents, content...)
		hashes = append(hashes, hash)
		content = content[:0]
		hash = hash[:0]
	}
	if err := rows.Err(); err != nil {
		log.Println(err)
		return nil, nil, false
	}

	sort.Sort(sliceslice(hashes))
	if d.sdchDictChunks == nil || len(d.sdchDictChunks) == 0 {
		return contents, hashes, true
	}

	var uniq int
	for _, newHash := range hashes {
		var exactMatch bool
		sort.Search(len(d.sdchDictChunks), func(i int) bool {
			cmp := bytes.Compare(d.sdchDictChunks[i], newHash)
			if cmp == 0 {
				exactMatch = true
			}
			return cmp >= 0
		})

		if !exactMatch {
			uniq++
		}
	}

	ratio := float64(uniq) / float64(len(d.sdchDictChunks))
	log.Printf("Got %d uniques out of %d (%f%%)", uniq, len(d.sdchDictChunks), 100*ratio)
	return contents, hashes, ratio > float64(0.1)
}

func (d *Dict) Stats() string {
	return fmt.Sprintf("matched %d out of %d", d.totalBytesDup, d.totalBytesIn)
}

type sliceslice [][]byte

func (ss sliceslice) Len() int           { return len(ss) }
func (ss sliceslice) Less(i, j int) bool { return bytes.Compare(ss[i], ss[j]) < 0 }
func (ss sliceslice) Swap(i, j int)      { ss[i], ss[j] = ss[j], ss[i] }
