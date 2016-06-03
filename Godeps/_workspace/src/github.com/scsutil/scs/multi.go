package scs

import (
        "io"
        "sort"
        "bytes"
        "errors"
        "strconv"
        "crypto/md5"
        "encoding/hex"
        "encoding/xml"
        "encoding/base64"
)

type Multi struct {
        Bucket   *Bucket
        Key      string
        UploadId string
}

var listMultiMax = 1000

type listMultiResp struct {
        NextKeyMarker      string
        NextUploadIdMarker string
        IsTruncated        bool
        Upload             []Multi
        CommonPrefixes     []string `xml:"CommonPrefixes>Prefix"`
}

func (b *Bucket) ListMulti(prefix, delim string) (multis []*Multi, prefixes []string, err error) {
        params := map[string][]string{
                "uploads":     {""},
                "max-uploads": {strconv.FormatInt(int64(listMultiMax), 10)},
                "prefix":      {prefix},
                "delimiter":   {delim},
        }
        for attempt := attempts.Start(); attempt.Next(); {
                req := &request{
                        method: "GET",
                        bucket: b.Name,
                        params: params,
                }
                var resp listMultiResp
                err := b.SCS.query(req, &resp)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return nil, nil, err
                }
                for i := range resp.Upload {
                        multi := &resp.Upload[i]
                        multi.Bucket = b
                        multis = append(multis, multi)
                }
                prefixes = append(prefixes, resp.CommonPrefixes...)
                if !resp.IsTruncated {
                        return multis, prefixes, nil
                }
                params["key-marker"] = []string{resp.NextKeyMarker}
                params["upload-id-marker"] = []string{resp.NextUploadIdMarker}
                attempt = attempts.Start()
        }
        panic("unreachable")
}

func (b *Bucket) Multi(key, contType string) (*Multi, error) {
        multis, _, err := b.ListMulti(key, "")
        if err != nil && !hasCode(err, "NoSuchUpload") {
                return nil, err
        }
        for _, m := range multis {
                if m.Key == key {
                        return m, nil
                }
        }
        return b.InitMulti(key, contType)
}

func (b *Bucket) InitMulti(key string, contType string) (*Multi, error) {
        headers := map[string][]string{
                "Content-Type":   {contType},
                "Content-Length": {"0"},
        }
        params := map[string][]string{
                "uploads": {""},
        }
        req := &request{
                method:  "POST",
                bucket:  b.Name,
                path:    key,
                headers: headers,
                params:  params,
        }
        var err error
        var resp struct {
                UploadId string `xml:"UploadId"`
        }
        for attempt := attempts.Start(); attempt.Next(); {
                err = b.SCS.query(req, &resp)
                if !shouldRetry(err) {
                        break
                }
        }
        if err != nil {
                return nil, err
        }
        return &Multi{Bucket: b, Key: key, UploadId: resp.UploadId}, nil
}


func (m *Multi) PutPart(n int, r io.ReadSeeker) (Part, error) {
        partSize, md5hex, _, err := seekerInfo(r)
        if err != nil {
                return Part{}, err
        }
        return m.putPart(n, r, partSize, md5hex)
}

func (m *Multi) putPart(n int, r io.ReadSeeker, partSize int64, md5 string) (Part, error) {
        headers := map[string][]string{
                "Content-Length": {strconv.FormatInt(partSize, 10)},
                "Content-MD5":    {md5},
        }
        params := map[string][]string{
                "uploadId":   {m.UploadId},
                "partNumber": {strconv.FormatInt(int64(n), 10)},
        }
        for attempt := attempts.Start(); attempt.Next(); {
                _, err := r.Seek(0, 0)
                if err != nil {
                        return Part{}, err
                }
                req := &request{
                        method:  "PUT",
                        bucket:  m.Bucket.Name,
                        path:    m.Key,
                        headers: headers,
                        params:  params,
                        payload: r,
                }
                err = m.Bucket.SCS.prepare(req)
                if err != nil {
                        return Part{}, err
                }
                resp, err := m.Bucket.SCS.run(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return Part{}, err
                }
                etag := resp.Header.Get("ETag")
                if etag == "" {
                        return Part{}, errors.New("part upload succeeded with no ETag")
                }
                return Part{n, etag, partSize}, nil
        }
        panic("unreachable")
}

func seekerInfo(r io.ReadSeeker) (size int64, md5hex string, md5b64 string, err error) {
        _, err = r.Seek(0, 0)
        if err != nil {
                return 0, "", "", err
        }
        digest := md5.New()
        size, err = io.Copy(digest, r)
        if err != nil {
                return 0, "", "", err
        }
        sum := digest.Sum(nil)
        md5hex = hex.EncodeToString(sum)
        md5b64 = base64.StdEncoding.EncodeToString(sum)
        return size, md5hex, md5b64, nil
}

type Part struct {
        N    int `xml:"PartNumber"`
        ETag string
        Size int64
}

type partSlice []Part

func (s partSlice) Len() int           { return len(s) }
func (s partSlice) Less(i, j int) bool { return s[i].N < s[j].N }
func (s partSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type listPartsResp struct {
        NextPartNumberMarker string
        IsTruncated          bool
        Part                 []Part
}

func (m *Multi) ListParts() ([]Part, error) {
        params := map[string][]string{
                "uploadId":  {m.UploadId},
        }
        var parts partSlice
        for attempt := attempts.Start(); attempt.Next(); {
                req := &request{
                        method: "GET",
                        bucket: m.Bucket.Name,
                        path:   m.Key,
                        params: params,
                }
                var resp = listPartsResp{}
                err := m.Bucket.SCS.query(req, &resp)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return nil, err
                }
                parts = append(parts, resp.Part...)
                if !resp.IsTruncated {
                        sort.Sort(parts)
                        return parts, nil
                }
                params["part-number-marker"] = []string{resp.NextPartNumberMarker}
                attempt = attempts.Start()
        }
        panic("unreachable")
}

type ReaderAtSeeker interface {
        io.ReaderAt
        io.ReadSeeker
}

func (m *Multi) PutAll(r ReaderAtSeeker, partSize int64) ([]Part, error) {
        old, err := m.ListParts()

        if err != nil && !hasCode(err, "NoSuchKey") {
                return nil, err
        }
        reuse := 0
        current := 1
        totalSize, err := r.Seek(0, 2)
        if err != nil {
                return nil, err
        }
        first := true
        var result []Part
        NextSection:
        for offset := int64(0); offset < totalSize || first; offset += partSize {
                first = false
                if offset+partSize > totalSize {
                        partSize = totalSize - offset
                }
                section := io.NewSectionReader(r, offset, partSize)
                _, md5hex, _, err := seekerInfo(section)
                if err != nil {
                        return nil, err
                }
                for reuse < len(old) && old[reuse].N <= current {
                        part := &old[reuse]
                        etag := `"` + md5hex + `"`
                        if part.N == current && part.Size == partSize && part.ETag == etag {
                                result = append(result, *part)
                                current++
                                continue NextSection
                        }
                        reuse++
                }

                part, err := m.putPart(current, section, partSize, md5hex)
                if err != nil {
                        return nil, err
                }
                result = append(result, part)
                current++
        }
        return result, nil
}

type completeUpload struct {
        XMLName xml.Name      `xml:"CompleteMultipartUpload"`
        Parts   completeParts `xml:"Part"`
}

type completePart struct {
        PartNumber int
        ETag       string
}

type completeParts []completePart

func (p completeParts) Len() int           { return len(p) }
func (p completeParts) Less(i, j int) bool { return p[i].PartNumber < p[j].PartNumber }
func (p completeParts) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Complete assembles the given previously uploaded parts into the
// final object. This operation may take several minutes.
//
// See http://goo.gl/2Z7Tw for details.
func (m *Multi) Complete(parts []Part) error {
        params := map[string][]string{
                "uploadId": {m.UploadId},
        }
        c := completeUpload{}
        for _, p := range parts {
                c.Parts = append(c.Parts, completePart{p.N, p.ETag})
        }
        sort.Sort(c.Parts)
        data, err := xml.Marshal(&c)
        if err != nil {
                return err
        }
        for attempt := attempts.Start(); attempt.Next(); {
                req := &request{
                        method:  "POST",
                        bucket:  m.Bucket.Name,
                        path:    m.Key,
                        params:  params,
                        payload: bytes.NewReader(data),
                }
                err := m.Bucket.SCS.query(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                return err
        }
        panic("unreachable")
}

// Abort deletes an unifinished multipart upload and any previously
// uploaded parts for it.
//
// After a multipart upload is aborted, no additional parts can be
// uploaded using it. However, if any part uploads are currently in
// progress, those part uploads might or might not succeed. As a result,
// it might be necessary to abort a given multipart upload multiple
// times in order to completely free all storage consumed by all parts.
//
// NOTE: If the described scenario happens to you, please report back to
// the goamz authors with details. In the future such retrying should be
// handled internally, but it's not clear what happens precisely (Is an
// error returned? Is the issue completely undetectable?).
//
// See http://goo.gl/dnyJw for details.
func (m *Multi) Abort() error {
        params := map[string][]string{
                "uploadId": {m.UploadId},
        }
        for attempt := attempts.Start(); attempt.Next(); {
                req := &request{
                        method: "DELETE",
                        bucket: m.Bucket.Name,
                        path:   m.Key,
                        params: params,
                }
                err := m.Bucket.SCS.query(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                return err
        }
        panic("unreachable")
}

type multiDeleteQueryData struct {
        XMLName     xml.Name        `xml:"Delete"`
        Quiet       bool            `xml:"Quiet"`
        Objects     []deleteObject  `xml:"Object"`
}
type deleteObject struct {
        Key         string      `xml:"Key"`
}
type MultiDeleteResponse struct {
        XMLName     xml.Name    `xml:"DeleteResult"`
        Deleted     []DeletedObject         `xml:"Deleted"`
        Failed      []DeleteFailedObject    `xml:"Error"`
}
type DeletedObject struct {
        Key         string      `xml:"Key"`
}
type DeleteFailedObject struct {
        Key     string      `xml:"Key"`
        Code    string      `xml:"Code"`
        Message string      `xml:"Message"`
}

func (bucket *Bucket) MultiDelete(objects []string) (response *MultiDeleteResponse, err error) {
        deleteData := multiDeleteQueryData{}
        deleteData.Quiet = false
        for _, object := range objects {
                deleteData.Objects = append(deleteData.Objects,
                        deleteObject{Key : object })
        }

        var innerBuffer []byte
        buffer := bytes.NewBuffer(innerBuffer)
        _, err = buffer.Write([]byte(xml.Header))
        if err != nil {
                return
        }
        err = xml.NewEncoder(buffer).Encode(deleteData)
        if err != nil {
                return
        }

        req := &request{
                method: "POST",
                bucket: bucket.Name,
                payload: buffer,
                params: map[string][]string{"delete" : {""} },
        }
        response = &MultiDeleteResponse{}
        for attempt := attempts.Start(); attempt.Next(); {
                err = bucket.SCS.query(req, response)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                return
        }
        return
}