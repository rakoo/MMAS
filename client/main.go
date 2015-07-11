package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/textproto"
	"os"
	"os/exec"
	"path"

	"github.com/elazarl/goproxy"
	"github.com/kr/pretty"
)

var (
	dictName string
)

func downloadDict(url string) {
	log.Println("Getting dict", path.Base(url))
	resp, err := http.Get(url)
	if err != nil {
		log.Println("Error getting dict:", err)
		return
	}
	f, err := os.Create(path.Join("dicts", path.Base(url)))
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	buffered := bufio.NewReader(resp.Body)
	tr := textproto.NewReader(buffered)
	sdchHeader, err := tr.ReadMIMEHeader()
	if err != nil {
		log.Println(err)
		return
	}
	pretty.Println("Decoded sdch header:", sdchHeader)

	_, err = io.Copy(f, buffered)
	if err != nil {
		log.Println(err)
		return
	}
	dictName = path.Base(url)
	log.Println("Got dict", dictName)
}

func main() {
	proxy := goproxy.NewProxyHttpServer()

	proxy.OnRequest().DoFunc(func(r *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
		r.Header.Add("Accept-Encoding", "sdch")

		if len(dictName) > 0 {
			rawId, err := hex.DecodeString(dictName)
			if err != nil {
				log.Println(err)
				return r, nil
			}
			uaId := base64.URLEncoding.EncodeToString(rawId[:6])
			r.Header.Set("Avail-Dictionary", uaId)
		}
		return r, nil
	})

	proxy.OnResponse().DoFunc(func(r *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
		dictUrl := r.Header.Get("Get-Dictionary")
		if dictUrl == "" {
			return r
		}

		dictName := path.Base(dictUrl)
		_, err := os.Stat(path.Join("dicts", dictName))
		if err != nil {
			if os.IsNotExist(err) {
				downloadDict(dictUrl)
			}
			return r
		}

		return r
	})

	proxy.OnResponse(goproxy.ContentTypeIs("sdch")).DoFunc(func(r *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
		var origBody bytes.Buffer
		tr := bufio.NewReader(io.TeeReader(r.Body, &origBody))

		serverId, err := tr.ReadString(byte(0))
		if err != nil {
			log.Println(err)
			r.Body = ioutil.NopCloser(io.MultiReader(&origBody, r.Body))
			return r
		}
		// Chop off the last 0x00
		serverId = serverId[:len(serverId)-1]
		rawServerId, err := base64.URLEncoding.DecodeString(serverId)
		if err != nil {
			log.Println(err)
			r.Body = ioutil.NopCloser(io.MultiReader(&origBody, r.Body))
			return r
		}
		ourDict, err := hex.DecodeString(dictName)
		if err != nil {
			log.Println(err)
			r.Body = ioutil.NopCloser(io.MultiReader(&origBody, r.Body))
			return r
		}
		if bytes.Compare(rawServerId, ourDict[6:12]) != 0 {
			r.Body = ioutil.NopCloser(io.MultiReader(&origBody, r.Body))
			return r
		}

		cmd := exec.Command("vcdiff", "patch", "-dictionary", path.Join("dicts/", dictName), "-stats")
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stdin = tr

		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			log.Println(err)
		}

		log.Println("[VCDIFF]", stderr.String())

		// TODO: send original content type in headers
		r.Header.Set("Content-Type", "text/html")
		r.Body = ioutil.NopCloser(&out)
		return r
	})

	proxy.OnRequest().HandleConnect(goproxy.AlwaysMitm)

	os.Mkdir("dicts", 0755)
	dir, err := os.Open("dicts")
	if err != nil {
		log.Fatal(err)
	}
	fis, err := dir.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}
	if len(fis) > 0 {
		last := fis[0]
		for _, fi := range fis[1:] {
			if fi.ModTime().After(last.ModTime()) {
				os.Remove(path.Join("dicts", last.Name()))
				last = fi
			}
		}
		dictName = last.Name()
	}

	log.Println("Let's go!")
	log.Fatal(http.ListenAndServe(":8081", proxy))
}
