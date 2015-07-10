package main

import (
	"bytes"
	"database/sql"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"

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
	DICT_PATH   = "/var/tmp/mmas-dict/"
	CHUNKS_PATH = "/var/tmp/mmas-chunks"
)

type bodyHandler struct {
	db           *sql.DB
	dictFileName string
	preums       []byte
}

func (bh *bodyHandler) handle(r *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
	acceptedEncodings := ctx.Req.Header["Accept-Encoding"]
	canSdch := false
	for _, enc := range acceptedEncodings {
		if enc == "sdch" {
			canSdch = true
			break
		}
	}
	if !canSdch {
		return r
	}

	oldBody := r.Body

	newBody, err := func() (io.ReadCloser, error) {
		content, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}

		var newBody io.ReadCloser
		if len(bh.dictFileName) > 0 {
			compressedBodyContent, err := bh.makeDiff(content)
			if err != nil {
				log.Println("[MAKEDIFF]", err)
				return nil, err
			}
			if len(compressedBodyContent) < len(content) {
				newBody = ioutil.NopCloser(bytes.NewBuffer(compressedBodyContent))
			}
		}

		changedPreums, err := bh.parseResponse(content)
		if err != nil {
			return nil, err
		}

		if changedPreums {
			err = bh.makeDict()
			if err != nil {
				return nil, err
			}
		}

		return newBody, nil
	}()

	if err != nil {
		log.Println(err)
		r.Body = oldBody
	} else {
		r.Body = newBody
	}
	return r
}

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

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS chunks (
		content BLOB,
		hash BLOB UNIQUE ON CONFLICT REPLACE,
		count INTEGER
	);`)
	if err != nil {
		log.Fatal(err)
	}

	var preums []byte
	err = db.QueryRow(`SELECT hash FROM chunks ORDER BY count, hash DESC LIMIT 1`).Scan(&preums)
	if err != nil && err != sql.ErrNoRows {
		log.Fatal(err)
	}

	bh := &bodyHandler{
		db:     db,
		preums: preums,
	}
	proxy.OnResponse(goproxy.ContentTypeIs("text/html")).DoFunc(bh.handle)

	err = os.Mkdir(DICT_PATH, 0755)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	dir, err := os.Open(DICT_PATH)
	if err != nil {
		log.Fatal(err)
	}
	fis, err := dir.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}
	sort.Sort(byDateInv(fis))

	// TODO: check sha with name
	if len(fis) > 0 {
		bh.dictFileName = path.Join(DICT_PATH, fis[0].Name())
		for _, fi := range fis[1:] {
			err := os.Remove(path.Join(DICT_PATH, fi.Name()))
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	prevNonProxyHandler := proxy.NonproxyHandler
	proxy.NonproxyHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_dictionary") {
			dictQuery := strings.Replace(r.URL.Path, "/_dictionary/", "", 1)
			parts := strings.Split(dictQuery, "/")
			if len(parts) != 2 {
				log.Println("Wrong query:", dictQuery)
				http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
				return
			}

			hostPort := parts[0]
			dictName := parts[1]

			dict, modTime, err := bh.makeSdchDict(hostPort, dictName)
			if err != nil {
				log.Println(err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/x-sdch/dictionary")
			http.ServeContent(w, r, "", modTime, dict)
			return
		}
		prevNonProxyHandler.ServeHTTP(w, r)
	})

	log.Println("Let's go !")
	log.Fatal(http.ListenAndServe(":8080", proxy))
}

type byDateInv []os.FileInfo

func (b byDateInv) Len() int           { return len(b) }
func (b byDateInv) Less(i, j int) bool { return b[i].ModTime().After(b[j].ModTime()) }
func (b byDateInv) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
