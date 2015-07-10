package main

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
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

var (
	statsBytesSent     uint64
	statsBytesOriginal uint64
)

type bodyHandler struct {
	db           *sql.DB
	dictFileName string
	topChunk     []byte
}

func (bh *bodyHandler) handle(r *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
	content, err := ioutil.ReadAll(r.Body)
	r.Body = ioutil.NopCloser(bytes.NewReader(content))

	if err != nil {
		return r
	}

	if len(bh.dictFileName) > 0 {

		// Build Get-Dictionary header
		hostport := r.Request.Host

		// Assuming no ipv6 here
		if !strings.Contains(r.Request.Host, ":") {
			hostport = hostport + ":80"
		}
		dictName := path.Base(bh.dictFileName)
		dictUrl := fmt.Sprintf("http://localhost:8080/_dictionary/%s/%s", hostport, dictName)
		r.Header.Set("Get-Dictionary", dictUrl)

		// Check if client can SDCH
		acceptedEncodings := ctx.Req.Header["Accept-Encoding"]
		canSdch := false
		for _, enc := range acceptedEncodings {
			if enc == "sdch" {
				canSdch = true
				break
			}
		}
		if canSdch {
			// Like Chromium, we only take the first one
			availDict := ctx.Req.Header.Get("Avail-Dictionary")
			uaId, err := base64.URLEncoding.DecodeString(availDict)
			if err != nil {
				log.Println(err)
				return r
			}

			rawDict, err := hex.DecodeString(dictName)
			if err != nil {
				log.Println(err)
				return r
			}
			if bytes.Compare(rawDict[:6], uaId) != 0 {
				return r
			}

			var newBody io.ReadCloser
			compressedBodyContent, err := bh.makeDiff(content)
			if err != nil {
				log.Println("[MAKEDIFF]", err)
				return r
			}
			if len(compressedBodyContent) < len(content) {
				r.Header.Set("Content-Type", "sdch")
				newBody = ioutil.NopCloser(bytes.NewBuffer(compressedBodyContent))
				r.Body = newBody

				statsBytesSent += uint64(len(compressedBodyContent))
				statsBytesOriginal += uint64(len(content))

				saved := 100 * (1 - float64(statsBytesSent)/float64(statsBytesOriginal))
				log.Printf("Reduced bytes on wire by %f %%\n", saved)
			}
		}
	}

	// TODO: Do the rest asynchronously
	changed, err := bh.parseResponse(content)
	if err != nil {
		log.Println(err)
		return r
	}

	if changed {
		err = bh.makeDict()
		if err != nil {
			log.Println(err)
			return r
		}
	}

	return r
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

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS chunks (
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

func human(in float64) string {
	switch {
	case in > 1024*1024:
		return fmt.Sprintf("%.2f MB", in/float64(1024*1024))
	case in > 1024:
		return fmt.Sprintf("%.2f kB", in/float64(1024))
	default:
		return fmt.Sprintf("%.2f B", in)
	}
}
