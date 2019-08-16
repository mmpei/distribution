package nos

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/distribution/context"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	log "github.com/Sirupsen/logrus"
)

const driverName = "nos"

// minChunkSize defines the minimum multipart upload chunk size
// OSS API requires multipart upload chunks to be at least 5MB
const minChunkSize = 5 << 20

const defaultChunkSize = 2 * minChunkSize

// listMax is the largest amount of objects you can request from OSS in a list call
const listMax = 1000

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey string
	SecretKey string
	Bucket    string
	ChunkSize int64
	Endpoint  string
	Ncdn      string
	NcdnRepo  []string
}

func init() {
	factory.Register(driverName, &nosDriverFactory{})
}

// nosDriverFactory implements the factory.StorageDriverFactory interface
type nosDriverFactory struct{}

func (factory *nosDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Client    NosClient
	Bucket    string
	ChunkSize int64
	Endpoint  string
	Ncdn      string
	NcdnRepo  []string

	pool  sync.Pool // pool []byte buffers used for WriteStream
	zeros []byte    // shared, zero-valued buffer used for WriteStream
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by NOS
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {

	accessKey, ok := parameters["accessKey"]
	if !ok {
		return nil, fmt.Errorf("No accesskey parameter provided")
	}
	secretKey, ok := parameters["secretKey"]
	if !ok {
		return nil, fmt.Errorf("No secretKey parameter provided")
	}

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
			}
			chunkSize = vv
		case int64:
			chunkSize = v
		case int, uint, int32, uint32, uint64:
			chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
		default:
			return nil, fmt.Errorf("invalid value for chunksize: %v", chunkSizeParam)
		}

		if chunkSize < minChunkSize {
			return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
		}
	}

	endpoint, ok := parameters["endpoint"]
	if !ok {
		endpoint = ""
	}

	ncdn, ok := parameters["ncdn"]
	if !ok {
		ncdn = ""
	}

	ncdnRepo := []string{}
	if ncdnRepoParam, ok := parameters["ncdn_repo"]; ok {
		switch v := ncdnRepoParam.(type) {
		case []interface{}:
			for _, repo := range v {
				ncdnRepo = append(ncdnRepo, fmt.Sprint(repo))
			}
		default:
			return nil, fmt.Errorf("invalid value for ncdn_repo: %v", ncdnRepoParam)
		}
	}

	params := DriverParameters{
		AccessKey: fmt.Sprint(accessKey),
		SecretKey: fmt.Sprint(secretKey),
		Bucket:    fmt.Sprint(bucket),
		ChunkSize: chunkSize,
		Endpoint:  fmt.Sprint(endpoint),
		Ncdn:      fmt.Sprint(ncdn),
		NcdnRepo:  ncdnRepo,
	}

	return New(params)
}

// New constructs a new Driver with the given NOS credentials, bucket, chunksize flag
func New(params DriverParameters) (*Driver, error) {

	client := NosClient{
		accessKey: params.AccessKey,
		secretKey: params.SecretKey,
		endPoint:  params.Endpoint,
	}

	d := &driver{
		Client:    client,
		Bucket:    params.Bucket,
		ChunkSize: params.ChunkSize,
		Endpoint:  params.Endpoint,
		Ncdn:      params.Ncdn,
		NcdnRepo:  params.NcdnRepo,
		zeros:     make([]byte, params.ChunkSize),
	}

	d.pool.New = func() interface{} {
		return make([]byte, d.ChunkSize)
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {

	body, err := d.Client.GetObject(d.Bucket, d.nosPath(path))
	if err != nil {
		return nil, parseError(path, err)
	}
	defer body.Close()

	content, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}

	return content, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {

	err := d.Client.PutObject(d.Bucket, d.nosPath(path), contents)
	if err != nil {
		return err
	}

	return nil
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	body, err := d.Client.GetObjectRange(d.Bucket, d.nosPath(path), "bytes="+strconv.FormatInt(offset, 10)+"-")
	if err != nil {
		return nil, err
	}

	return body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, apd bool) (storagedriver.FileWriter, error) {
	key := d.nosPath(path)
	if !apd {
		// TODO (brianbland): cancel other uploads at this path
		multi, err := d.Client.InitMultiUpload(d.Bucket, key)
		if err != nil {
			return nil, err
		}
		mu := initMulti(multi)
		return d.newWriter(key, mu, nil), nil
	}
	multis, err := d.Client.ListMulti(d.Bucket, key, "", "") //nos not support, get all MultiUploads
	if err != nil {
		return nil, parseError(path, err)
	}
	for _, multi := range multis {
		if key != multi.Key {
			continue
		}
		parts, err := d.Client.ListUploadParts(d.Bucket, key, multi.UploadId, 1000, 0)
		if err != nil {
			return nil, parseError(path, err)
		}
		var multiSize int64
		for _, part := range parts.Parts {
			multiSize += int64(part.Size)
		}
		mu := newMulti(multi)
		return d.newWriter(key, mu, parts.Parts), nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	// try to fetch Object by GetObject interface. ListObjects is too expensive.
	fileInfo, err := d.fetchFileInfo(path)
	if err == nil && fileInfo != nil {
		return storagedriver.FileInfoInternal{FileInfoFields: *fileInfo}, nil
	} else if err == nil && fileInfo == nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	log.Infof("Stat: use nos headObject failed, will use ListObject. Error: %v", err)
	listObjectsResult, err := d.Client.ListObjects(d.Bucket, d.nosPath(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(listObjectsResult.Contents) == 1 {
		if listObjectsResult.Contents[0].Key != d.nosPath(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = listObjectsResult.Contents[0].Size

			timestamp, err := time.Parse("2006-01-02T15:04:05 +0800", listObjectsResult.Contents[0].LastModified)
			if err != nil {
				return nil, err
			}
			fi.ModTime = timestamp
		}
	} else if len(listObjectsResult.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {

	if path != "/" && path[len(path)-1] != '/' {
		path = path + "/"
	}

	listObjectsResult, err := d.Client.ListObjects(d.Bucket, d.nosPath(path), "/", "", listMax)
	if err != nil {
		return nil, err
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range listObjectsResult.Contents {
			files = append(files, strings.Replace(key.Key, "", "/", 1))
		}

		for _, commonPrefix := range listObjectsResult.CommonPrefixes {
			prefix := commonPrefix.Prefix
			directories = append(directories, strings.Replace(prefix[0:len(prefix)-1], "", "/", 1))
		}

		if listObjectsResult.IsTruncated {
			listObjectsResult, err = d.Client.ListObjects(d.Bucket, d.nosPath(path), "/", listObjectsResult.NextMarker, listMax)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {

	err := d.Client.MoveObject(d.Bucket, d.nosPath(sourcePath), d.nosPath(destPath))
	if err != nil {
		return err
	}

	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {

	listObjectsResult, err := d.Client.ListObjects(d.Bucket, d.nosPath(path), "", "", listMax)
	if err != nil || len(listObjectsResult.Contents) == 0 {
		return storagedriver.PathNotFoundError{Path: path}
	}

	deleteMultiObjects := DeleteMultiObjects{Quiet: true}
	for len(listObjectsResult.Contents) > 0 {
		for _, key := range listObjectsResult.Contents {
			deleteMultiObjects.Append(DeleteObject{Key: key.Key})
		}

		err = d.Client.DeleteMultiObjects(d.Bucket, deleteMultiObjects)
		if err != nil {
			return err
		}

		// if there is not contents left, break
		if !listObjectsResult.IsTruncated {
			break
		}

		listObjectsResult, err = d.Client.ListObjects(d.Bucket, d.nosPath(path), "", "", listMax)
		if err != nil {
			return err
		}
	}

	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// TODO
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	//return "http://" + d.Endpoint + "/" + d.Bucket + "/" + d.nosPath(path), nil
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresTime := time.Now().Add(20 * time.Minute)

	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}
	newPath := "http://" + d.Bucket + "." + d.Endpoint + "/" + d.nosPath(path)
	signedURL, err := d.Client.generateSignedURL(methodString, newPath, expiresTime)
	if err != nil {
		return "", err
	}
	return signedURL, nil
}

func parseError(path string, err error) error {
	// tomcat will remove reason phrase in release 8.0, but add a switch to add this label in 8.5.
	// and in release 9.0 this phrase will be removed forever.
	// todo should use statusCode to judge the response but status
	//if err.Error() == "404 Not Found" {
	if strings.HasPrefix(err.Error(), "404") {
		return storagedriver.PathNotFoundError{Path: path}
	}

	return err
}

func (d *driver) nosPath(path string) string {
	return strings.TrimLeft(path, "/")
}

func (d *driver) fetchFileInfo(path string) (*storagedriver.FileInfoFields, error) {
	// if path end with this string means path is a file
	var filePathSuffix = []string{"/data", "/link", "/startedat"}

	fi := &storagedriver.FileInfoFields{
		Path: path,
		IsDir: false,
	}
	result, err := d.Client.HeadObject(d.Bucket, d.nosPath(path))
	//log.Infof("fetchFileInfo %+v: %+v", result, err)
	if err != nil {
		return nil, err
	} else if result == nil { // 404: key not found
		// check if path is a file, return not found
		for _, suffix := range filePathSuffix {
			if strings.HasSuffix(path, suffix) {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("directory, should list")
	}

	fi.Size = result.Size
	fi.ModTime = result.ModTime
	return fi, nil
}

// getbuf returns a buffer from the driver's pool with length d.ChunkSize.
func (d *driver) getbuf() []byte {
	return d.pool.Get().([]byte)
}

func (d *driver) putbuf(p []byte) {
	copy(p, d.zeros)
	d.pool.Put(p)
}

// writer attempts to upload parts to nos in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver      *driver
	key         string
	multi       *Multi
	parts       []UploadPartRet
	size        int64
	readyPart   []byte
	pendingPart []byte
	closed      bool
	committed   bool
	cancelled   bool
}

func (d *driver) newWriter(key string, multi *Multi, parts []UploadPartRet) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += int64(part.Size)
	}
	return &writer{
		driver: d,
		key:    key,
		multi:  multi,
		parts:  parts,
		size:   size,
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	// If the last written part is smaller than minChunkSize, we need to make a
	// new multipart upload :sadface:
	if len(w.parts) > 0 && int(w.parts[len(w.parts)-1].Size) < minChunkSize {
		_, err := w.driver.Client.CompleteMultiUpload(w.driver.Bucket, w.key, w.multi.UploadId, *TranslateToUploadParts(w.parts))
		if err != nil {
			w.driver.Client.AbortMultiUpload(w.driver.Bucket, w.key, w.multi.UploadId)
			return 0, err
		}

		multi, err := w.driver.Client.InitMultiUpload(w.driver.Bucket, w.key)
		if err != nil {
			return 0, err
		}
		w.multi = initMulti(multi)

		// If the entire written file is smaller than minChunkSize, we need to make
		// a new part from scratch :double sad face:
		if w.size < minChunkSize {
			contents, err := w.driver.Client.GetObject(w.driver.Bucket, w.key)
			if err != nil {
				return 0, err
			}
			defer contents.Close()
			w.parts = nil
			contents.Read(w.readyPart)
		} else {
			// Otherwise we can use the old file as the new first part
			part, err := w.driver.Client.UploadPartCopy(w.driver.Bucket, w.key, w.multi.UploadId, 1, w.key, "")
			if err != nil {
				return 0, err
			}
			w.parts = []UploadPartRet{{PartNumber: int(part.PartNumber), Etag: part.Etag, Size: int(part.Size)}}
		}
	}

	var n int

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part
		if neededBytes := int(w.driver.ChunkSize) - len(w.readyPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.readyPart = append(w.readyPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
			} else {
				w.readyPart = append(w.readyPart, p...)
				n += len(p)
				p = nil
			}
		}

		if neededBytes := int(w.driver.ChunkSize) - len(w.pendingPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.pendingPart = append(w.pendingPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
				err := w.flushPart()
				if err != nil {
					w.size += int64(n)
					return n, err
				}
			} else {
				w.pendingPart = append(w.pendingPart, p...)
				n += len(p)
				p = nil
			}
		}
	}
	w.size += int64(n)
	return n, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	err := w.driver.Client.AbortMultiUpload(w.driver.Bucket, w.key, w.multi.UploadId)
	return err
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true
	_, err = w.driver.Client.CompleteMultiUpload(w.driver.Bucket, w.key, w.multi.UploadId, *TranslateToUploadParts(w.parts))
	if err != nil {
		w.driver.Client.AbortMultiUpload(w.driver.Bucket, w.key, w.multi.UploadId)
		return err
	}
	return nil
}

// flushPart flushes buffers to write a part to S3.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart() error {
	if len(w.readyPart) == 0 && len(w.pendingPart) == 0 {
		// nothing to write
		return nil
	}
	if len(w.pendingPart) < int(w.driver.ChunkSize) {
		// closing with a small pending part
		// combine ready and pending to avoid writing a small part
		w.readyPart = append(w.readyPart, w.pendingPart...)
		w.pendingPart = nil
	}
	part, err := w.driver.Client.UploadPart(w.driver.Bucket, w.key, w.multi.UploadId, int32(len(w.parts)+1), bytes.NewReader(w.readyPart))
	if err != nil {
		return err
	}
	w.parts = append(w.parts, UploadPartRet{PartNumber: int(part.PartNumber), Etag: part.Etag, Size: int(part.Size)})
	w.readyPart = w.pendingPart
	w.pendingPart = nil
	return nil
}

func TranslateToUploadParts(uploadPartIn []UploadPartRet) (*UploadParts) {
	parts := UploadParts{}
	for _, part := range uploadPartIn {
		parts.Append(UploadPart{PartNumber: int32(part.PartNumber), Etag: part.Etag})
	}
	return &parts
}