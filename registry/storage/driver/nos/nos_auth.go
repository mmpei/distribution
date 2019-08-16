package nos

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"sort"
	"strings"

	_ "github.com/Sirupsen/logrus"
)

var subResources map[string]bool = map[string]bool{
	"acl":           true,
	"location":      true,
	"versioning":    true,
	"versions":      true,
	"versionId":     true,
	"uploadId":      true,
	"uploads":       true,
	"partNumber":    true,
	"delete":        true,
	"deduplication": true,
}

func signRequest(request *http.Request, secretKey string) string {

	stringToSign := ""
	stringToSign += (request.Method + "\n")
	stringToSign += (request.Header.Get("Content-MD5") + "\n")
	stringToSign += (request.Header.Get("Content-Type") + "\n")
	stringToSign += (request.Header.Get("Date") + "\n")

	// Parse Canonicalized Headers (Just handle x-nos-move-source for now)
	if request.Header.Get("x-nos-move-source") != "" {
		stringToSign += ("x-nos-move-source:" + request.Header.Get("x-nos-move-source") + "\n")
	}

	// Parse Canonicalized Resources
	stringToSign += (getResource(request.URL.Host, request.URL.Path))

	request.ParseForm()

	var keys sort.StringSlice
	for key := range request.Form {
		if _, ok := subResources[key]; ok {
			keys = append(keys, key)
		}
	}
	keys.Sort()

	for i := 0; i < keys.Len(); i++ {
		if i == 0 {
			stringToSign += "?"
		}
		stringToSign += keys[i]
		if val := request.Form[keys[i]]; val[0] != "" {
			stringToSign += ("=" + val[0])
		}

		if i < keys.Len()-1 {
			stringToSign += "&"
		}
	}

	// calculate hmac-sha256 signature
	key := []byte(secretKey)
	h := hmac.New(sha256.New, key)
	h.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func signUrl(request *http.Request, expires string, secretKey string) string {

	stringToSign := ""
	stringToSign += (request.Method + "\n")
	stringToSign += ("" + "\n")
	stringToSign += ("" + "\n")
	stringToSign += (expires + "\n")

	// Parse Canonicalized Resources
	stringToSign += (getResource(request.URL.Host, request.URL.Path))

	request.ParseForm()

	var keys sort.StringSlice
	for key := range request.Form {
		if _, ok := subResources[key]; ok {
			keys = append(keys, key)
		}
	}
	keys.Sort()

	for i := 0; i < keys.Len(); i++ {
		if i == 0 {
			stringToSign += "?"
		}
		stringToSign += keys[i]
		if val := request.Form[keys[i]]; val[0] != "" {
			stringToSign += ("=" + val[0])
		}

		if i < keys.Len()-1 {
			stringToSign += "&"
		}
	}

	// calculate hmac-sha256 signature
	key := []byte(secretKey)
	h := hmac.New(sha256.New, key)
	h.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func getResource(host string, path string) string {

	splits := strings.SplitN(host, ".", 2)
	splitsPath := strings.SplitN(path, "/", 2)

	resource := ""
	if len(splitsPath) == 1 || splitsPath[1] == "" {
		resource = "/" + splits[0] + "/"
	} else {
		resource = "/" + splits[0] + "/" + url.QueryEscape(splitsPath[1])
	}

	return resource
}

