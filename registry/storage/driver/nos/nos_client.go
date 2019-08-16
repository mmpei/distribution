package nos

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
	"net/url"
	"strings"
	"github.com/docker/distribution/uuid"
	log "github.com/Sirupsen/logrus"
)

const (
	RFC1123_NOS = "Mon, 02 Jan 2006 15:04:05 Asia/Shanghai"
	RFC1123_NOS_GMT = "Mon, 02 Jan 2006 15:04:05 GMT"
	MAX_TYR_TIMES = 8
)

type NosClient struct {
	endPoint string

	accessKey string
	secretKey string

	httpClient http.Client
}

func (client *NosClient) HeadObject(bucket, object string) (*HeadObjectResult, error) {

	request, err := client.getNosRequest("HEAD", "http://"+bucket+"."+client.endPoint+"/"+object, nil, nil)
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient HeadObject request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient HeadObject err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient HeadObject resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		contentLength := resp.Header.Get("Content-Length")
		lastModified := resp.Header.Get("Last-Modified")

		size, err := strconv.ParseInt(contentLength, 10, 64)
		if err != nil {
			return nil, err
		}

		// GMT is UTC time
		modTime, err := time.Parse(RFC1123_NOS_GMT, lastModified)
		if err != nil {
			// RFC1123 is CST time, should reduce 8 hours
			modTime, err = time.Parse(RFC1123_NOS, lastModified)
			if err != nil {
				return nil, err
			}
			modTime.Add(-8 * time.Hour)
		}

		return &HeadObjectResult{Size: size, ModTime: modTime}, nil
	} else if resp.StatusCode == http.StatusNotFound { // not found
		return nil, nil
	} else {
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) PutObject(bucket, object string, content []byte) error {

	request, err := client.getNosRequest("PUT", "http://"+bucket+"."+client.endPoint+"/"+object, nil, bytes.NewReader(content))
	if err != nil {
		return err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient PutObject request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient PutObject err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return err
	}
	log.Debugf("NosClient PutObject resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		return errors.New(resp.Status)
	}
}

func (client *NosClient) DeleteObject(bucket, object string) error {

	request, err := client.getNosRequest("DELETE", "http://"+bucket+"."+client.endPoint+"/"+object, nil, nil)
	if err != nil {
		return err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient DeleteObject request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient DeleteObject err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return err
	}
	log.Debugf("NosClient DeleteObject resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
		return nil
	} else {
		return errors.New(resp.Status)
	}
}

func (client *NosClient) DeleteMultiObjects(bucket string, delectObjects DeleteMultiObjects) error {

	params := map[string]string{
		"delete": "1",
	}

	body, err := xml.Marshal(delectObjects)
	if err != nil {
		return err
	}

	request, err := client.getNosRequest("POST", "http://"+bucket+"."+client.endPoint, params, bytes.NewReader(body))
	if err != nil {
		return err
	}

	md5Ctx := md5.New()
	md5Ctx.Write(body)
	cipherStr := md5Ctx.Sum(nil)

	request.Header.Set("Content-MD5", hex.EncodeToString(cipherStr))
	request.Header.Set("Authorization", "NOS "+client.accessKey+":"+signRequest(request, client.secretKey)) // TODO

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient DeleteMultiObjects request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient DeleteMultiObjects err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return err
	}
	log.Debugf("NosClient DeleteMultiObjects resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		return errors.New(resp.Status)
	}
}

func (client *NosClient) GetObject(bucket, object string) (io.ReadCloser, error) {

	request, err := client.getNosRequest("GET", "http://"+bucket+"."+client.endPoint+"/"+object, nil, nil)
	//log.Infof("NosClient GetObject request : %+v", request)
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient GetObject request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient GetObject err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient GetObject resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)

	if resp.StatusCode == http.StatusOK {
		return resp.Body, nil
	} else {
		ioutil.ReadAll(resp.Body)
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) MoveObject(bucket, srcObject, destObject string) error {

	request, err := client.getNosRequest("PUT", "http://"+bucket+"."+client.endPoint+"/"+destObject, nil, nil)
	if err != nil {
		return err
	}

	request.Header.Set("x-nos-move-source", "/"+bucket+"/"+srcObject)
	request.Header.Set("Authorization", "NOS "+client.accessKey+":"+signRequest(request, client.secretKey)) // TODO

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient MoveObject request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient MoveObject err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return err
	}
	log.Debugf("NosClient MoveObject resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		return errors.New(resp.Status)
	}
}

func (client *NosClient) GetObjectRange(bucket, object, objRange string) (io.ReadCloser, error) {

	request, err := client.getNosRequest("GET", "http://"+bucket+"."+client.endPoint+"/"+object, nil, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Range", objRange)

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient GetObjectRange request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient GetObjectRange request %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient GetObjectRange status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)

	if resp.StatusCode == http.StatusPartialContent {
		return resp.Body, nil
	} else {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) InitMultiUpload(bucket, object string) (*InitMultiUploadResult, error) {
	params := map[string]string{
		"uploads": "1",
	}

	request, err := client.getNosRequest("POST", "http://"+bucket+"."+client.endPoint+"/"+object, params, nil)
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient InitMultiUpload request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient InitMultiUpload request %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient InitMultiUpload status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		result := &InitMultiUploadResult{}
		err = parseXmlBody(resp.Body, result)
		if err != nil {
			return nil, err
		}

		return result, nil
	} else {
		ioutil.ReadAll(resp.Body)
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) UploadPart(bucket, object string, uploadId int64, partNumber int32, content io.Reader) (*UploadPart, error) {

	params := map[string]string{
		"uploadId":   strconv.FormatInt(uploadId, 10),
		"partNumber": strconv.FormatInt(int64(partNumber), 10),
	}

	request, err := client.getNosRequest("PUT", "http://"+bucket+"."+client.endPoint+"/"+object, params, content)
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient UploadPart request %s: %+v", logUUID, request)
	size := request.ContentLength
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient UploadPart err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient UploadPart resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		etag := resp.Header.Get("Etag")
		if etag == "" {
			return nil, errors.New("No Etag Header")
		}

		return &UploadPart{PartNumber: partNumber, Etag: etag, Size: size}, nil
	} else {
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) UploadPartCopy(bucket, object string, uploadId int64, partNumber int32, srcObject, srcObjRange string) (*UploadPart, error) {

	var srcBody io.ReadCloser
	var err error

	if srcObjRange == "" {
		srcBody, err = client.GetObjectRange(bucket, srcObject, srcObjRange)
	} else {
		srcBody, err = client.GetObject(bucket, srcObject)
	}
	if err != nil {
		return nil, err
	}
	defer srcBody.Close()

	return client.UploadPart(bucket, object, uploadId, partNumber, srcBody)
}

func (client *NosClient) CompleteMultiUpload(bucket, object string, uploadId int64, uploadParts UploadParts) (*CompleteMultipartUploadResult, error) {

	params := map[string]string{
		"uploadId": strconv.FormatInt(uploadId, 10),
	}

	body, err := xml.Marshal(uploadParts)
	if err != nil {
		return nil, err
	}

	request, err := client.getNosRequest("POST", "http://"+bucket+"."+client.endPoint+"/"+object, params, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient CompleteMultiUpload request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient CompleteMultiUpload err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient CompleteMultiUpload status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		result := &CompleteMultipartUploadResult{}
		err = parseXmlBody(resp.Body, result)
		if err != nil {
			return nil, err
		}

		return result, nil
	} else {
		ioutil.ReadAll(resp.Body)
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) AbortMultiUpload(bucket, object string, uploadId int64) error {

	params := map[string]string{
		"uploadId": strconv.FormatInt(uploadId, 10),
	}

	request, err := client.getNosRequest("DELETE", "http://"+bucket+"."+client.endPoint+"/"+object, params, nil)
	if err != nil {
		return err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient AbortMultiUpload request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient AbortMultiUpload err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return err
	}
	log.Debugf("NosClient AbortMultiUpload status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		return nil
	} else {
		return errors.New(resp.Status)
	}
}

func (client *NosClient) ListUploadParts(bucket, object string, uploadId int64, maxParts int32, marker int32) (*ListPartsResult, error) {

	params := map[string]string{
		"uploadId": strconv.FormatInt(uploadId, 10),
	}

	request, err := client.getNosRequest("GET", "http://"+bucket+"."+client.endPoint+"/"+object, params, nil)
	//log.Infof("NosClient ListUploadParts request : %+v", request)
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient ListUploadParts request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient ListUploadParts err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient ListUploadParts resp %s status code: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		result := &ListPartsResult{}
		err = parseXmlBody(resp.Body, result)
		if err != nil {
			return nil, err
		}

		return result, nil
	} else {
		ioutil.ReadAll(resp.Body)
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) ListObjects(bucket, prefix, delimiter, marker string, maxKeys int) (*ListObjectsResult, error) {

	params := map[string]string{
		"prefix":    prefix,
		"delimiter": delimiter,
		"marker":    marker,
		"max-keys":  strconv.Itoa(maxKeys),
	}
	//log.Infof("NosClient ListObjects to getNosRequest request params: %+v", params)
	request, err := client.getNosRequest("GET", "http://"+bucket+"."+client.endPoint, params, nil)
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient ListObjects request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient ListObjects err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient ListObjects status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		result := &ListObjectsResult{}
		err = parseXmlBody(resp.Body, result)
		if err != nil {
			return nil, err
		}

		return result, nil
	} else {
		ioutil.ReadAll(resp.Body)
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) ListMulti(bucket, prefix, delimiter string, keyMarker string) (multis []*Upload, err error) {
	i := MAX_TYR_TIMES
	for i > 0 {
		result, err := client.ListMultiResult(bucket, keyMarker)
		if err != nil {
			return nil, err
		}

		for i := range result.Uploads {
			multi := &result.Uploads[i]
			if strings.HasPrefix(multi.Key, prefix) {
				multis = append(multis, multi)
			}
		}

		if !result.IsTruncated {
			return multis, nil
		}
		keyMarker = result.NextKeyMarker
		i--

		if i < 3 {
			log.Warningf("NosClient ListMulti inited uploadId count has > 6000, should clean InitMultiUploadId")
		}
	}
	log.Errorf("NosClient ListMulti inited uploadId count has > 8000, should clean InitMultiUploadId")
	return multis, nil
}

func (client *NosClient) ListMultiResult(bucket, keyMarker string) (result *ListMultiResult, err error) {

	params := map[string]string{
		"uploads": "1",
		"key-marker": keyMarker,
		"max-uploads": strconv.Itoa(1000),
	}

	request, err := client.getNosRequest("GET", "http://"+bucket+"."+client.endPoint, params, nil)
	if err != nil {
		return nil, err
	}

	logUUID := uuid.Generate().String()
	begin := time.Now()
	log.Debugf("NosClient ListMultiResult request %s: %+v", logUUID, request)
	resp, err := client.httpClient.Do(request)
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Errorf("NosClient ListMultiResult err %s: %+v, time:%dms", logUUID, err, duration/1e6)
		return nil, err
	}
	log.Debugf("NosClient ListMultiResult resp status code %s: %d, time:%dms", logUUID, resp.StatusCode, duration/1e6)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		result := &ListMultiResult{}
		err = parseXmlBody(resp.Body, result)
		if err != nil {
			return nil, err
		}
		return result, nil
	} else {
		ioutil.ReadAll(resp.Body)
		return nil, errors.New(resp.Status)
	}
}

func (client *NosClient) getNosRequest(method, urlStr string, params map[string]string, body io.Reader) (*http.Request, error) {

	validParams := map[string]string{}
	for key, val := range params {
		if val != "" {
			validParams[key] = val
		}
	}

	if len(validParams) > 0 {
		urlStr += "?"
	}

	i := 0
	for key, val := range validParams {
		urlStr += key
		if val != "" {
			urlStr += ("=" + val)
		}

		if i < len(validParams)-1 {
			urlStr += "&"
		}

		i++
	}

	request, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Date", time.Now().UTC().Add(8 * time.Hour).Format(RFC1123_NOS))
	request.Header.Set("Authorization", "NOS "+client.accessKey+":"+signRequest(request, client.secretKey))

	return request, nil
}

func (client *NosClient) generateSignedURL(method, path string, expires time.Time) (string, error) {
	//var uv = url.Values{}

	request, err := http.NewRequest(method, path, nil)
	if err != nil {
		return "", err
	}

	exp := strconv.FormatInt(expires.Unix(), 10)
	//uv.Set("Expires", exp)
	//uv.Set("NOSAccessKeyId", client.accessKey)
	//uv.Set("Signature", url.QueryEscape(signUrl(request, exp, client.secretKey)))

	urlString := path + "?" + "Expires=" + exp + "&NOSAccessKeyId=" + client.accessKey + "&Signature=" +
		url.QueryEscape(signUrl(request, exp, client.secretKey))
	return urlString, nil
}

func parseXmlBody(body io.Reader, value interface{}) error {

	content, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}

	err = xml.Unmarshal(content, value)
	if err != nil {
		return err
	}

	return nil
}
