package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"time"
)

var (
	errNoChange = errors.New("No change")
)

func (bh *bodyHandler) makeDict(reqHost string) error {
	log.Println("Will make dict")
	start := time.Now()
	rows, err0 := bh.db.Query(`SELECT content FROM chunks ORDER BY count, content DESC`)
	if err0 != nil {
		return err0
	}

	var host, port string
	// Assuming no ipv6 here
	if strings.Contains(reqHost, ":") {
		var err error
		host, port, err = net.SplitHostPort(reqHost)
		if err != nil {
			return err
		}
	} else {
		host = reqHost
		port = "80"
	}

	err := func() error {
		hash := sha256.New()

		var headerBuf bytes.Buffer
		headerMw := io.MultiWriter(&headerBuf, hash)
		host = "reddit.com"
		fmt.Fprintf(headerMw, "Domain: .%s\n", host)
		fmt.Fprint(headerMw, "Path: /\n")
		fmt.Fprint(headerMw, "Format-Version: 1.0\n")
		fmt.Fprintf(headerMw, "Port: %s\n", port)
		fmt.Fprint(headerMw, "Max-Age: 86400\n\n")

		var contentBuf bytes.Buffer
		contentMw := io.MultiWriter(&contentBuf, hash)
		for rows.Next() {
			var content []byte
			err := rows.Scan(&content)
			if err != nil {
				return err
			}
			_, err = contentMw.Write(content)
			if err != nil {
				return err
			}
		}

		if err := rows.Err(); err != nil {
			return err
		}

		hashHex := hex.EncodeToString(hash.Sum(nil))
		newFileName := path.Join(DICT_PATH, hashHex)
		if newFileName == bh.DictName() {
			return errNoChange
		}

		err := ioutil.WriteFile(newFileName, contentBuf.Bytes(), 0644)
		if err != nil {
			return err
		}
		newHdrFileName := path.Join(DICT_HDR_PATH, hashHex)
		err = ioutil.WriteFile(newHdrFileName, headerBuf.Bytes(), 0644)
		if err != nil {
			return err
		}

		oldName := bh.DictName()
		bh.SetDictName(newFileName)
		bh.SetDictHdrName(newHdrFileName)
		if len(oldName) > 0 {
			err := os.Remove(oldName)
			if err != nil {
				return err
			}
			err = os.Remove(path.Join(DICT_HDR_PATH, path.Base(oldName)))
			if err != nil {
				return err
			}
		}
		return nil
	}()

	if err == errNoChange {
		log.Println("No change")
		return nil
	}

	if err != nil {
		return err
	}

	st, err := os.Stat(bh.DictName())
	if err != nil {
		return err
	}

	log.Printf("Generated a %d bytes dict in %f msecs\n", st.Size(), time.Since(start).Seconds()*1000)
	return nil
}

func (bh bodyHandler) makeSdchDict() (dict io.ReadSeeker, modTime time.Time, err error) {

	dictHdr, err1 := os.Open(bh.DictHdrName())
	dictContent, err2 := os.Open(bh.DictName())
	if err1 != nil || err2 != nil {
		return nil, time.Time{}, fmt.Errorf("Couldn't read files: %s -- %s", err1, err2)
	}
	st, err := dictContent.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}

	var buf bytes.Buffer
	io.Copy(&buf, dictHdr)
	io.Copy(&buf, dictContent)
	return bytes.NewReader(buf.Bytes()), st.ModTime(), nil
}

type sdchDictHeader struct {
}
