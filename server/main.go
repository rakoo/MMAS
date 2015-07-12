package main

import (
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/rakoo/mmas/pkg/dict"
)

type SDCHProxy struct {
	proxy *httputil.ReverseProxy
	d     *dict.Dict
	u     *url.URL
}

func newSDCHProxy(target *url.URL) SDCHProxy {
	iproxy := httputil.NewSingleHostReverseProxy(target)
	pDirector := iproxy.Director
	iproxy.Director = func(r *http.Request) {
		pDirector(r)
		r.Host = r.URL.Host
	}

	d, err := dict.New()
	if err != nil {
		log.Fatal(err)
	}
	return SDCHProxy{
		proxy: iproxy,
		d:     d,
		u:     target,
	}
}

func (s SDCHProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	canSdch := false

	aes := r.Header["Accept-Encoding"]
	for _, ae := range aes {
		split := strings.Split(ae, ",")
		for _, each := range split {
			if strings.TrimSpace(each) == "sdch" {
				canSdch = true
			}
		}
	}

	if !canSdch {
		s.proxy.ServeHTTP(w, r)
		return
	}

	rr := httptest.NewRecorder()
	s.proxy.ServeHTTP(rr, r)
	copyHeader(w.Header(), rr.Header())

	isTextHtml := false
	cts := rr.Header()["Content-Type"]
	for _, ct := range cts {
		if strings.HasPrefix(ct, "text/html") {
			isTextHtml = true
		}
	}

	if !isTextHtml {
		io.Copy(w, rr.Body)
		return
	}

	// Read content, ungzip it if needed
	originalContent := rr.Body.Bytes()
	workContent := originalContent

	ces := rr.Header()["Content-Encoding"]
	hasGzip := false
	for _, ce := range ces {
		if ce == "gzip" {
			hasGzip = true
			break
		}
	}
	if hasGzip {
		gzr, err := gzip.NewReader(rr.Body)
		if err != nil {
			httpError(w)
			return
		}
		workContent, err = ioutil.ReadAll(gzr)
		if err != nil {
			httpError(w)
			return
		}
	}

	err := s.d.Eat(workContent)
	if err != nil {
		log.Println("Error eating:", err)
		// If all else fails, return original response
		w.Write(originalContent)
		return
	}

	//log.Println(s.d.Stats())

	// If all else fails, return original response
	w.Write(originalContent)
}

// Same as httputil/reverseproxy.go
func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func httpError(w http.ResponseWriter) {
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

func main() {
	u, err := url.Parse("https://en.wikipedia.org/")
	if err != nil {
		log.Fatal(err)
	}
	proxy := newSDCHProxy(u)

	log.Println("Let's go !")
	log.Fatal(http.ListenAndServe(":8080", proxy))
}
