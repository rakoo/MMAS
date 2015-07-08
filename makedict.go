package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"time"
)

func (bh *bodyHandler) makeDict() error {
	start := time.Now()
	rows, err := bh.db.Query(`SELECT count, content FROM chunks ORDER BY count, content DESC`)
	if err != nil {
		return err
	}

	allReaders := make([]io.Reader, 0)
	for rows.Next() {
		var count int
		var content []byte
		err := rows.Scan(&count, &content)
		if err != nil {
			return err
		}
		for i := 0; i < count; i++ {
			allReaders = append(allReaders, bytes.NewReader(content))
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	tmp, err := ioutil.TempFile(DICT_PATH, "mmas-dict-")
	if err != nil {
		return err
	}
	tmp.Close()

	cmd := exec.Command("vcdiff", "encode", "-dictionary", "/dev/zero", "-target_matches", "-delta", tmp.Name())
	cmd.Stderr = os.Stderr
	cmd.Stdin = io.MultiReader(allReaders...)
	if err = cmd.Run(); err != nil {
		return err
	}

	// Making defer work for us
	var hash []byte
	var size int64
	err = func() error {
		f, err := os.Open(tmp.Name())
		if err != nil {
			return err
		}
		defer f.Close()

		sha := sha256.New()
		_, err = io.Copy(sha, f)
		if err != nil {
			return err
		}
		hash = sha.Sum(nil)

		st, err := f.Stat()
		if err != nil {
			return err
		}
		size = st.Size()
		return nil
	}()

	if err != nil {
		return err
	}

	newFileName := path.Join(DICT_PATH, hex.EncodeToString(hash))
	err = os.Rename(tmp.Name(), newFileName)
	if err != nil {
		return err
	}

	if len(bh.dictFileName) > 0 {
		err = os.Remove(bh.dictFileName)
		if err != nil {
			return err
		}
		bh.dictFileName = newFileName
	}

	log.Printf("Generated a %d bytes dict in %f msecs\n", size, time.Since(start).Seconds()*1000)
	return nil
}
