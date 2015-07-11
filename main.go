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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elazarl/goproxy"
	"github.com/kr/pretty"

	_ "github.com/mattn/go-sqlite3"
)

const (
	DICT_PATH     = "/var/tmp/mmas-dict/"
	DICT_HDR_PATH = "/var/tmp/mmas-dict-hdr/"
	CHUNKS_PATH   = "/var/tmp/mmas-chunks"
)

var (
	statsBytesSent     uint64
	statsBytesOriginal uint64
)

type bodyHandler struct {
	db       *sql.DB
	topChunk []byte

	mu              sync.Mutex
	dictFileName    string
	dictHdrFileName string
}

func (bh *bodyHandler) DictName() string {
	var ret string
	bh.mu.Lock()
	ret = bh.dictFileName
	bh.mu.Unlock()
	return ret
}

func (bh *bodyHandler) DictHdrName() string {
	var ret string
	bh.mu.Lock()
	ret = bh.dictHdrFileName
	bh.mu.Unlock()
	return ret
}

func (bh *bodyHandler) SetDictName(name string) {
	bh.mu.Lock()
	bh.dictFileName = name
	bh.mu.Unlock()
}

func (bh *bodyHandler) SetDictHdrName(name string) {
	bh.mu.Lock()
	bh.dictHdrFileName = name
	bh.mu.Unlock()
}

func (bh *bodyHandler) handle(r *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
	// Set it to not-sdch-encoded by default
	r.Header.Set("X-Sdch-Encode", "0")

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return r
	}

	go func() {
		log.Println("Parsing against", r.Request.Host, r.Request.URL.Path)
		_, err = bh.parseResponse(content)
		if err != nil {
			log.Println("Error parsing content:", err)
			return
		}

		if len(bh.DictName()) == 0 {
			err = bh.makeDict(r.Request.Host)
			if err != nil {
				log.Println("Error making dict:", err)
				return
			}
		}
	}()

	r.Body = ioutil.NopCloser(bytes.NewReader(content))

	if len(bh.DictName()) > 0 {

		// Build Get-Dictionary header
		hostport := r.Request.Host

		// Assuming no ipv6 here
		if !strings.Contains(r.Request.Host, ":") {
			hostport = hostport + ":80"
		}
		dictName := path.Base(bh.DictName())
		dictUrl := fmt.Sprintf("/_dictionary/%s/%s", hostport, dictName)
		r.Header.Set("Get-Dictionary", dictUrl)

		// Check if client can SDCH
		acceptedEncodings := ctx.Req.Header["Accept-Encoding"]
		canSdch := false
		for _, line := range acceptedEncodings {
			for _, enc := range strings.Split(line, ",") {
				if strings.TrimSpace(enc) == "sdch" {
					canSdch = true
					break
				}
			}
		}
		if !canSdch {
			return r
		}

		pretty.Println(r.Request.Header)
		// Like Chromium, we only take the first one
		availDicts := r.Request.Header.Get("Avail-Dictionary")
		split := strings.Split(availDicts, ",")
		if split[0] == "" {
			return r
		}

		uaId, err := base64.URLEncoding.DecodeString(strings.TrimSpace(split[0]))
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
			r.Header.Del("X-Sdch-Encode")
			newBody = ioutil.NopCloser(bytes.NewBuffer(compressedBodyContent))
			r.Body = newBody

			statsBytesSent += uint64(len(compressedBodyContent))
			statsBytesOriginal += uint64(len(content))

			saved := 100 * (1 - float64(statsBytesSent)/float64(statsBytesOriginal))
			log.Printf("Reduced bytes on wire by %f %%\n", saved)
		}
	}

	return r
}

var last = time.Now()

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

	matchPath := regexp.MustCompile("reddit.com")
	proxy.OnResponse(
		goproxy.ContentTypeIs("text/html"),
		goproxy.ReqHostMatches(matchPath),
	).DoFunc(bh.handle)

	err = os.Mkdir(DICT_PATH, 0755)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}

	err = os.Mkdir(DICT_HDR_PATH, 0755)
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

	if len(fis) > 0 {
		current := fis[0]
		for _, fi := range fis[1:] {
			if fi.ModTime().After(current.ModTime()) {
				err := os.Remove(path.Join(DICT_PATH, fi.Name()))
				if err != nil {
					log.Fatal(err)
				}
				err = os.Remove(path.Join(DICT_HDR_PATH, fi.Name()))
				if err != nil {
					log.Fatal(err)
				}
				current = fi
			}
		}
		bh.SetDictName(path.Join(DICT_PATH, current.Name()))
		bh.SetDictHdrName(path.Join(DICT_HDR_PATH, current.Name()))
	}

	proxy.OnRequest().DoFunc(func(r *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
		if !strings.HasPrefix(r.URL.Path, "/_dictionary") {
			return r, nil
		}

		dictQuery := strings.Replace(r.URL.Path, "/_dictionary/", "", 1)
		parts := strings.Split(dictQuery, "/")
		if len(parts) != 2 {
			log.Println("Wrong query:", dictQuery)
			resp := goproxy.NewResponse(r, "text/plain", http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return nil, resp
		}

		dictName := parts[1]

		if dictName != path.Base(bh.DictName()) {
			log.Println(err)
			resp := goproxy.NewResponse(r, "text/plain", http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return nil, resp
		}

		dict, modTime, err := bh.makeSdchDict()
		if err != nil {
			log.Println(err)
			resp := goproxy.NewResponse(r, "text/plain", http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return nil, resp
		}

		size, err1 := dict.Seek(0, os.SEEK_END)
		_, err2 := dict.Seek(0, os.SEEK_SET)
		if err1 != nil || err2 != nil {
			log.Println(err, err2)
			resp := goproxy.NewResponse(r, "text/plain", http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return nil, resp
		}

		resp := &http.Response{}
		resp.Request = r
		resp.TransferEncoding = r.TransferEncoding
		resp.Header = make(http.Header)
		resp.Header.Add("Content-Type", "application/x-sdch-dictionary")
		resp.Header.Add("Content-Length", strconv.Itoa(int(size)))
		resp.Header.Add("X-Sdch-Encode", "0")
		resp.Header.Add("Date", time.Now().Format(time.RFC1123))
		resp.Header.Add("Expires", time.Now().Add(1*time.Hour).Format(time.RFC1123))
		resp.Header.Add("Last-Modified", modTime.Format(time.RFC1123))

		resp.StatusCode = 200
		resp.ContentLength = size
		resp.Body = ioutil.NopCloser(dict)

		log.Println("Sending back dict")
		return nil, resp
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
