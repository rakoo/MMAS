package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"time"
)

func (bh *bodyHandler) makeDict() error {
	start := time.Now()
	rows, err0 := bh.db.Query(`SELECT content FROM chunks ORDER BY count, content DESC`)
	if err0 != nil {
		return err0
	}

	err := func() error {
		var buf bytes.Buffer
		hash := sha256.New()
		mw := io.MultiWriter(&buf, hash)
		for rows.Next() {
			var content []byte
			err := rows.Scan(&content)
			if err != nil {
				return err
			}
			_, err = mw.Write(content)
			if err != nil {
				return err
			}
		}

		if err := rows.Err(); err != nil {
			return err
		}

		newFileName := path.Join(DICT_PATH, hex.EncodeToString(hash.Sum(nil)))
		err := ioutil.WriteFile(newFileName, buf.Bytes(), 0644)
		if err != nil {
			return err
		}

		if len(bh.dictFileName) > 0 {
			oldName := bh.dictFileName

			err := os.Remove(oldName)
			if err != nil {
				return err
			}
		} else {
			bh.dictFileName = newFileName
		}
		return nil
	}()

	if err != nil {
		return err
	}

	st, err := os.Stat(bh.dictFileName)
	if err != nil {
		return err
	}

	log.Printf("Generated a %d bytes dict in %f msecs\n", st.Size(), time.Since(start).Seconds()*1000)
	return nil
}

func (bh bodyHandler) makeSdchDict(hostPort, dictName string) (dict io.ReadSeeker, modTime time.Time, err error) {

	if dictName != path.Base(bh.dictFileName) {
		return nil, time.Time{}, fmt.Errorf("Not found!")
	}

	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return nil, time.Time{}, err
	}
	if port == "" {
		port = "80"
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Domain: %s\n", host)
	fmt.Fprint(&buf, "Path: /\n")
	fmt.Fprint(&buf, "Format-Version: 1.0\n")
	fmt.Fprintf(&buf, "Port: %s\n", port)
	fmt.Fprint(&buf, "Max-Age: 86400\n\n")

	dictFile, err := os.Open(bh.dictFileName)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer dictFile.Close()
	_, err = io.Copy(&buf, dictFile)
	if err != nil {
		return nil, time.Time{}, err
	}
	st, err := dictFile.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}

	return bytes.NewReader(buf.Bytes()), st.ModTime(), nil
}

type sdchDictHeader struct {
}
