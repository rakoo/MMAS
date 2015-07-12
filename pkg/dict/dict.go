package dict

import (
	"crypto/sha1"
	"database/sql"
	"fmt"
	"log"

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

type Dict struct {
	db *sql.DB

	// stats
	totalBytesDup uint64
	totalBytesIn  uint64
}

func New() (*Dict, error) {
	db, err := sql.Open("sqlite3", "dict")
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS chunks (
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

func (d *Dict) Eat(content []byte) error {
	rs := rollsum.New()
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(sqlUpSert)
	if err != nil {
		return err
	}

	var match uint64
	q := content
	buf := make([]byte, 0)
	for len(q) > 0 {
		b := q[0]
		q = q[1:]

		rs.Roll(b)
		d.totalBytesIn++

		buf = append(buf, b)
		if rs.OnSplitWithBits(5) {
			h := sha1.Sum(buf)

			var s uint64
			d.db.QueryRow(`SELECT LENGTH(content) FROM chunks WHERE hash = ?`, h[:]).Scan(&s)
			match += s

			_, err = stmt.Exec(buf, h[:], h[:])
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

	log.Printf("Matched %d out of %d\n", match, len(content))
	return nil
}

func (d *Dict) Stats() string {
	return fmt.Sprintf("matched %d out of %d", d.totalBytesDup, d.totalBytesIn)
}
