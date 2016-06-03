// Package scs provides a storagedriver.StorageDriver implementation to
// store blobs in Sohu cloud storage.
//
// Because scs is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// +build include_scs

package scs

import (
        "io"
        "fmt"
        "sync"
        "time"
        "bytes"
        "strings"
        "strconv"
        "reflect"
        "net/http"
        "io/ioutil"

        "github.com/scsutil/scs"

        "github.com/docker/distribution/context"
        storagedriver "github.com/docker/distribution/registry/storage/driver"
        "github.com/docker/distribution/registry/storage/driver/base"
        "github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "scs"

// minChunkSize defines the minimum multipart upload chunk size
// scs API requires multipart upload chunks to be at least 5MB
const minChunkSize = 5 << 20
const defaultChunkSize = 2 * minChunkSize

// listMax is the largest amount of objects you can request from scs in a list call
const listMax = 100

// DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
        Auth            scs.Auth
        Region          scs.Region
        Bucket          string
        ChunkSize       int64
}

func init() {
        factory.Register(driverName, &scsDriverFactory{})
}

// scsDriverFactory implements the factory.StorageDriverFactory interface
type scsDriverFactory struct{}

func (factory *scsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
        return FromParameters(parameters)
}

type driver struct {
        Client        *scs.SCS
        Bucket        *scs.Bucket
        ChunkSize     int64

        pool  sync.Pool // pool []byte buffers used for WriteStream
        zeros []byte    // shared, zero-valued buffer used for WriteStream
}

type baseEmbed struct {
        base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by scs
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
        baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - region
// - bucket
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
        accessKey, ok := parameters["accesskey"]
        if !ok {
                return nil, fmt.Errorf("No accesskey parameter provided")
        }
        secretKey, ok := parameters["secretkey"]
        if !ok {
                return nil, fmt.Errorf("No secretkey parameter provided")
        }
        regionName, ok := parameters["region"]
        if !ok || fmt.Sprint(regionName) == "" {
                return nil, fmt.Errorf("No region parameter provided")
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
                        return nil, fmt.Errorf("invalid value for chunksize: %#v", chunkSizeParam)
                }

                if chunkSize < minChunkSize {
                        return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
                }
        }

        params := DriverParameters{
                Auth:   scs.Auth{
                        AccessKey:  fmt.Sprint(accessKey),
                        SecretKey:  fmt.Sprint(secretKey),
                },
                Region: scs.Regions[fmt.Sprint(regionName)],
                Bucket: fmt.Sprint(bucket),
                ChunkSize: chunkSize,
        }

        return New(params)
}

// New constructs a new Driver with the given scs credentials, region and bucketName
func New(params DriverParameters) (*Driver, error) {
        client := scs.New(params.Auth, params.Region)
        bucket := client.Bucket(params.Bucket)

        // Validate that the given credentials have at least read permissions in the given bucket scope.
        if _, err := bucket.List("", "", "", 1); err != nil {
                return nil, err
        }

        d := &driver{
                Client:        client,
                Bucket:        bucket,
                ChunkSize:     params.ChunkSize,
                zeros:         make([]byte, params.ChunkSize),
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
        content, err := d.Bucket.Get(d.scsPath(path))
        if err != nil {
                if hasCode(err, "NoSuchKey") {
                        return nil, storagedriver.PathNotFoundError{Path: path}
                }
                return nil, err
        }
        return content, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
        err :=  d.Bucket.Put(d.scsPath(path), contents, d.getContentType())
        return err
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
        resp, err := d.Bucket.GetResponseParams(d.scsPath(path), nil, offset, -1)
        if err != nil {
                if hasCode(err, "NoSuchKey") {
                        return nil, storagedriver.PathNotFoundError{Path: path}
                }
                return nil, err
        }
        if resp.StatusCode != http.StatusPartialContent {
                resp.Body.Close()
                return ioutil.NopCloser(bytes.NewReader(nil)), nil
        }
        return resp.Body, nil
}

// WriteStream stores the contents of the provided io.Reader at a location designated by the given path.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {

        partNumber := 1
        bytesRead := 0
        var putErrChan chan error
        parts := []scs.Part{}
        var part scs.Part
        done := make(chan struct{})

        multi, err := d.Bucket.InitMulti(path, d.getContentType())
        if err != nil {
                return 0, err
        }
        buf := d.getbuf()

        defer func() {
                if putErrChan != nil {
                        if putErr := <-putErrChan; putErr != nil {
                                err = putErr
                        }
                }

                if len(parts) > 0 {
                        if multi == nil {
                                panic("Unreachable")
                        } else {
                                if errc := multi.Complete(parts); errc != nil {
                                        multi.Abort()
                                }
                        }
                }
                d.putbuf(buf)
                close(done)
        }()

        fromSmallCurrent := func(total int64) error {
                current, err := d.ReadStream(ctx, path, 0)
                if err != nil {
                        return err
                }

                bytesRead = 0
                for int64(bytesRead) < total {
                        nn, err := current.Read(buf[bytesRead:total])
                        bytesRead += nn
                        if err != nil {
                                if err != io.EOF {
                                        return err
                                }
                                break
                        }
                }
                return nil
        }

        fromReader := func(from int64) error {
                bytesRead = 0
                for from+int64(bytesRead) < d.ChunkSize {
                        nn, err := reader.Read(buf[from+int64(bytesRead):])
                        totalRead += int64(nn)
                        bytesRead += nn
                        if err != nil {
                                if err != io.EOF {
                                        return err
                                }
                                break
                        }
                }
                if putErrChan == nil {
                        putErrChan = make(chan error)
                } else {
                        if putErr := <-putErrChan; putErr != nil {
                                putErrChan = nil
                                return putErr
                        }
                }

                go func(bytesRead int, from int64, buf []byte) {
                        defer d.putbuf(buf)
                        defer func() {
                                select {
                                case putErrChan <- nil:
                                case <-done:
                                        return
                                }
                        }()
                        if bytesRead <= 0 {
                                return
                        }

                        var err error
                        var part scs.Part

                        loop:
                        for retries := 0; retries < 5; retries++ {
                                part, err = multi.PutPart(int(partNumber), bytes.NewReader(buf[0:int64(bytesRead)+from]))
                                if err == nil {
                                        break
                                }
                                switch err := err.(type) {
                                case *scs.Error:
                                        switch err.Code {
                                        case "RequestTimeout":
                                        default:
                                                break loop
                                        }
                                }

                                backoff := 100 * time.Millisecond * time.Duration(retries+1)
                                time.Sleep(backoff)
                        }

                        if err != nil {
                                select {
                                case putErrChan <- err:
                                case <-done:
                                        return
                                }
                        }
                        parts = append(parts, part)
                        partNumber++
                }(bytesRead, from, buf)

                buf = d.getbuf()
                return nil
        }

        if offset > 0 {
                resp, err := d.Bucket.Head(d.scsPath(path))
                if err != nil {
                        if ossErr, ok := err.(*scs.Error); !ok || ossErr.Code != "NoSuchKey" {
                                return 0, err
                        }
                }

                currentLength := int64(0)
                if err == nil {
                        currentLength = resp.ContentLength
                }

                if currentLength >= offset {
                        if offset < d.ChunkSize {
                                if err = fromSmallCurrent(offset); err != nil {
                                        return totalRead, err
                                }

                                if err = fromReader(offset); err != nil {
                                        return totalRead, err
                                }

                                if totalRead+offset < d.ChunkSize {
                                        return totalRead, nil
                                }
                        } else {
                                current, err := d.ReadStream(ctx, path, 0)
                                cur_buf := make([]byte, offset)
                                current.Read(cur_buf)
                                part, err = multi.PutPart(int(partNumber), bytes.NewReader(cur_buf))
                                if err != nil {
                                        return 0, err
                                }
                                parts = append(parts, part)
                                partNumber++
                        }
                } else {
                        fromZeroFillSmall := func(from, to int64) error {
                                bytesRead = 0
                                for from+int64(bytesRead) < to {
                                        nn, err := bytes.NewReader(d.zeros).Read(buf[from+int64(bytesRead) : to])
                                        bytesRead += nn
                                        if err != nil {
                                                return err
                                        }
                                }
                                return nil
                        }

                        fromZeroFillLarge := func(from, to int64) error {
                                bytesRead64 := int64(0)
                                for to-(from+bytesRead64) >= d.ChunkSize {
                                        part, err := multi.PutPart(int(partNumber), bytes.NewReader(d.zeros))
                                        if err != nil {
                                                return err
                                        }
                                        bytesRead64 += d.ChunkSize

                                        parts = append(parts, part)
                                        partNumber++
                                }
                                return fromZeroFillSmall(0, (to-from)%d.ChunkSize)
                        }

                        if currentLength < d.ChunkSize {
                                if offset < d.ChunkSize {
                                        if err = fromSmallCurrent(currentLength); err != nil {
                                                return totalRead, err
                                        }
                                        if err = fromZeroFillSmall(currentLength, offset); err != nil {
                                                return totalRead, err
                                        }
                                        if err = fromReader(offset); err != nil {
                                                return totalRead, err
                                        }
                                        if totalRead+offset < d.ChunkSize {
                                                return totalRead, nil
                                        }
                                } else {
                                        if err = fromSmallCurrent(currentLength); err != nil {
                                                return totalRead, err
                                        }
                                        if err = fromZeroFillSmall(currentLength, d.ChunkSize); err != nil {
                                                return totalRead, err
                                        }
                                        part, err = multi.PutPart(int(partNumber), bytes.NewReader(buf))
                                        if err != nil {
                                                return totalRead, err
                                        }
                                        parts = append(parts, part)
                                        partNumber++
                                        if err = fromZeroFillLarge(d.ChunkSize, offset); err != nil {
                                                return totalRead, err
                                        }
                                        if err = fromReader(offset % d.ChunkSize); err != nil {
                                                return totalRead, err
                                        }
                                        if totalRead+(offset%d.ChunkSize) < d.ChunkSize {
                                                return totalRead, nil
                                        }
                                }
                        } else {
                                current, err := d.ReadStream(ctx, path, 0)
                                cur_buf := make([]byte, currentLength)
                                current.Read(cur_buf)
                                part, err = multi.PutPart(int(partNumber), bytes.NewReader(cur_buf))
                                if err != nil {
                                        return 0, err
                                }
                                parts = append(parts, part)
                                partNumber++
                                if err = fromZeroFillLarge(currentLength, offset); err != nil {
                                        return totalRead, err
                                }
                                if err = fromReader((offset - currentLength) % d.ChunkSize); err != nil {
                                        return totalRead, err
                                }
                                if totalRead+((offset-currentLength)%d.ChunkSize) < d.ChunkSize {
                                        return totalRead, nil
                                }
                        }
                }
        }

        for {
                if err = fromReader(0); err != nil {
                        return totalRead, err
                }
                if int64(bytesRead) < d.ChunkSize {
                        break
                }
        }

        return totalRead, nil
}

// Stat retrieves the FileInfo for the given path, including the current size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
        listResponse, err := d.Bucket.List(d.scsPath(path), "", "", 1)
        if err != nil {
                return nil, err
        }

        fi := storagedriver.FileInfoFields{
                Path: path,
        }
        if len(listResponse.Contents) == 1 {
                if listResponse.Contents[0].Key != d.scsPath(path) {
                        fi.IsDir = true
                } else {
                        fi.IsDir = false
                        fi.Size = listResponse.Contents[0].Size
                        timestamp, err := time.Parse(time.RFC3339Nano, listResponse.Contents[0].LastModified)
                        if err != nil {
                                return nil, err
                        }
                        fi.ModTime = timestamp
                }
        } else if len(listResponse.CommonPrefixes) == 1 {
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
        prefix := ""
        if d.scsPath("") == "" {
                prefix = "/"
        }

        files := []string{}
        directories := []string{}
        if (strings.HasPrefix(path, "/catalog")) {
                path = strings.TrimPrefix(path, "/catalog")
                listResponse, err := d.Bucket.List(d.scsPath(path), "/_", "", listMax)
                if err != nil {
                        return nil, err
                }
                for {
                        for _, commonPrefix := range listResponse.CommonPrefixes {
                                directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-2], d.scsPath(""), prefix, 1))
                        }
                        if listResponse.IsTruncated {
                                listResponse, err = d.Bucket.List(d.scsPath(path), "/_", listResponse.NextMarker, listMax)
                                if err != nil {
                                        return nil, err
                                }
                        } else {
                                break
                        }
                }
                return directories, nil
        }

        listResponse, err := d.Bucket.List(d.scsPath(path), "/", "", listMax)
        if err != nil {
                return nil, err
        }
        for {
                for _, key := range listResponse.Contents {
                        files = append(files, strings.Replace(key.Key, d.scsPath(""), prefix, 1))
                }
                for _, commonPrefix := range listResponse.CommonPrefixes {
                        directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.scsPath(""), prefix, 1))
                }
                if listResponse.IsTruncated {
                        listResponse, err = d.Bucket.List(d.scsPath(path), "/", listResponse.NextMarker, listMax)
                        if err != nil {
                                return nil, err
                        }
                } else {
                        break
                }
        }

        return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
        err := d.Bucket.Rename(d.scsPath(sourcePath), d.scsPath(destPath))
        return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
        listResponse, err := d.Bucket.List(d.scsPath(path), "", "", listMax)
        if err != nil || len(listResponse.Contents) == 0 {
                return storagedriver.PathNotFoundError{Path: path}
        }

        scsObjects := make([]string, listMax)

        for len(listResponse.Contents) > 0 {
                for index, key := range listResponse.Contents {
                        scsObjects[index] = key.Key
                }

                _, err := d.Bucket.MultiDelete(scsObjects)
                if err != nil {
                        return nil
                }

                listResponse, err = d.Bucket.List(d.scsPath(path), "", "", listMax)
                if err != nil {
                        return err
                }
        }

        return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
        expiresTime := time.Now().Add(20 * time.Minute)
        expires, ok := options["expiry"]
        if ok {
                et, ok := expires.(time.Time)
                if ok {
                        expiresTime = et
                }
        }

        testURL := d.Bucket.SignedURL(d.scsPath(path), expiresTime)
        return testURL, nil
}


func (d *driver) scsPath(path string) string {
        return strings.TrimLeft(path, "/")
}

func (d *driver) getContentType() string {
        return "application/octet-stream"
}

func (d *driver) getbuf() []byte {
        return d.pool.Get().([]byte)
}

func (d *driver) putbuf(p []byte) {
        copy(p, d.zeros)
        d.pool.Put(p)
}

func hasCode(err error, code string) bool {
        scserr, ok := err.(*scs.Error)
        return ok && scserr.Code == code
}
