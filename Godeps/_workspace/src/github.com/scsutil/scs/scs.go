package scs

import (
        "io"
        "fmt"
        "net"
        "time"
        "bytes"
        "errors"
        "net/url"
        "strconv"
        "strings"
        "net/http"
        "io/ioutil"
        "encoding/xml"
        "encoding/json"
)

type SCS struct {
        Auth
        Region
        HTTPClient func() *http.Client

        private byte
        tmp_body []byte
}

type Bucket struct{
        *SCS
        Name string
        CreationDate string
        Region string
}

type Owner struct {
        ID string
        DisplayName string
}


var attempts = AttemptStrategy{
        Min:   5,
        Total: 5 * time.Second,
        Delay: 200 * time.Millisecond,
}

func New(auth Auth, region Region) *SCS {
        return &SCS{
                Auth: auth,
                Region: region,
                private: 0,
                HTTPClient: func() *http.Client {
                        return http.DefaultClient
                },
        }
}

func (scs *SCS) Bucket(name string) *Bucket{
        name = strings.ToLower(name)
        return &Bucket{scs, name, "", ""}
}

// The ListBucketsResp type holds the results of a List buckets operation.
type ListBucketsResp struct {
        Buckets []Bucket `xml:">Bucket"`
}

func (scs *SCS) ListBuckets() (result *ListBucketsResp, err error) {
        req := &request{
                path: "/",
        }
        result = &ListBucketsResp{}
        for attempt := attempts.Start(); attempt.Next(); {
                err = scs.query(req, result)
                if !shouldRetry(err) {
                        break
                }
        }
        if err != nil {
                return nil, err
        }
        // set SCS instance on buckets
        for i := range result.Buckets {
                result.Buckets[i].SCS = scs
        }
        return result, nil
}

func (b *Bucket) Path(path string) string {
        if !strings.HasPrefix(path, "/") {
                path = "/" + path
        }
        return "/" + b.Name + path
}

func (b *Bucket) GetBucketLocation() (region *Region, err error) {
        // analysis url scheme
        tmp_url, err := url.Parse(b.SCS.SCSEndpoint)
        if (err != nil) {
                return nil, err
        }
        scheme := tmp_url.Scheme

        // prepare request
        params := map[string][]string{
                "location": {""},
        }
        req := &request{
                bucket: b.Name,
                path:   "/",
                params: params,
        }
        err = b.SCS.prepare(req)
        if err != nil {
                return
        }

        // request
        for attempt := attempts.Start(); attempt.Next(); {
                resp, err := b.SCS.run(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return &BJCNC, err
                }
                // analysis result
                if len(resp.Header["X-Scs-Location"]) > 0 {
                        return &Region{"tmp", scheme + "://" + resp.Header["X-Scs-Location"][0], ""}, nil
                } else {
                        err := errors.New(" Server return region Url failed")
                        return &BJCNC, err
                }
        }
        panic("unreachable")
}

func (b *Bucket) PutBucket() error {
        req := &request{
                method:  "PUT",
                bucket:  b.Name,
                path:    "/",
        }
        return b.SCS.query(req, nil)
}

func (b *Bucket) DelBucket() (err error) {
        req := &request{
                method: "DELETE",
                bucket: b.Name,
                path:   "/",
        }
        for attempt := attempts.Start(); attempt.Next(); {
                err = b.SCS.query(req, nil)
                if !shouldRetry(err) {
                        break
                }
        }
        return err
}

func (b *Bucket) Get(path string) (data []byte, err error) {
        body, err := b.GetReader(path)
        if err != nil {
                return nil, err
        }
        data, err = ioutil.ReadAll(body)
        body.Close()
        return data, err
}

func (b *Bucket) GetReader(path string) (rc io.ReadCloser, err error) {
        resp, err := b.GetResponse(path)
        if resp != nil {
                return resp.Body, err
        }
        return nil, err
}

func (b *Bucket) GetResponse(path string) (*http.Response, error) {
        return b.GetResponseParams(path, nil, 0, -1)
}

func (b *Bucket) GetResponseParams(path string, params url.Values, rangestart, rangeEnd int64) (*http.Response, error) {
        var headers map[string][]string
        if rangestart == 0 && rangeEnd == -1 {
                headers = map[string][]string {}
        } else {
                if rangeEnd == -1 {
                        headers = map[string][]string{
                                "Range": {fmt.Sprintf("bytes=%d-", rangestart)},
                        }
                } else {
                        headers = map[string][]string{
                                "Range": {fmt.Sprintf("bytes=%d-%d", rangestart, rangeEnd)},
                        }
                }
        }

        req := &request{
                bucket: b.Name,
                path:   path,
                params: params,
                headers: headers,
        }
        err := b.SCS.prepare(req)
        if err != nil {
                return nil, err
        }
        for attempt := attempts.Start(); attempt.Next(); {
                resp, err := b.SCS.run(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return nil, err
                }
                return resp, nil
        }
        panic("unreachable")
}

func (b *Bucket) Head(path string) (*http.Response, error) {
        req := &request{
                method: "HEAD",
                bucket: b.Name,
                path:   path,
        }
        err := b.SCS.prepare(req)
        if err != nil {
                return nil, err
        }
        for attempt := attempts.Start(); attempt.Next(); {
                resp, err := b.SCS.run(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return nil, err
                }
                return resp, nil
        }
        panic("unreachable")
}


func (b *Bucket) Put(path string, data []byte, contType string) error {
        body := bytes.NewBuffer(data)
        return b.PutReader(path, body, int64(len(data)), contType)
}

func (b *Bucket) PutHeader(path string, data []byte, customHeaders map[string][]string) error {
        body := bytes.NewBuffer(data)
        return b.PutReaderHeader(path, body, int64(len(data)), customHeaders)
}

// PutReader inserts an object into the SCS bucket by consuming data
// from r until EOF.
func (b *Bucket) PutReader(path string, r io.Reader, length int64, contType string) error {
        headers := map[string][]string{
                "Content-Length": {strconv.FormatInt(length, 10)},
                "Content-Type":   {contType},
        }
        req := &request{
                method:  "PUT",
                bucket:  b.Name,
                path:    path,
                headers: headers,
                payload: r,
        }
        return b.SCS.query(req, nil)
}

func (b *Bucket) PutReaderHeader(path string, r io.Reader, length int64, customHeaders map[string][]string) error {
        // Default headers
        headers := map[string][]string{
                "Content-Length": {strconv.FormatInt(length, 10)},
                "Content-Type":   {"application/text"},
        }

        // Override with custom headers
        for key, value := range customHeaders {
                headers[key] = value
        }

        req := &request{
                method:  "PUT",
                bucket:  b.Name,
                path:    path,
                headers: headers,
                payload: r,
        }
        return b.SCS.query(req, nil)
}

func (b *Bucket) Copy(oldPath, newPath string) error {
        if !strings.HasPrefix(oldPath, "/") {
                oldPath = "/" + oldPath
        }

        req := &request{
                method: "PUT",
                bucket: b.Name,
                path:   newPath,
                headers: map[string][]string{
                        "x-scs-copy-source": {sohucsEscape("/" + b.Name + oldPath)},
                },
        }
        err := b.SCS.prepare(req)
        if err != nil {
                return err
        }
        for attempt := attempts.Start(); attempt.Next(); {
                _, err = b.SCS.run(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return err
                }
                return nil
        }
        panic("unreachable")
}

func (b *Bucket) Del(path string) error {
        req := &request{
                method: "DELETE",
                bucket: b.Name,
                path:   path,
        }
        return b.SCS.query(req, nil)
}

func (b *Bucket) Rename(oldObject string, newObject string) (err error) {
	req := &request {
		method: "PUT",
        	bucket: b.Name,
       	 	path:   oldObject,
        	params :    url.Values {
            		"internal-rename"   : nil,
            		"newObjectName"     : {newObject},
        	},
    	}
    	for attempt := attempts.Start(); attempt.Next(); {
        	err = b.SCS.query(req, nil)
        	if !shouldRetry(err) {
            		break
        	}
    	}
    	return err
}

// The ListResp type holds the results of a List bucket operation.
type ListResp struct {
        Name       string
        Prefix     string
        Delimiter  string
        Marker     string
        NextMarker string
        MaxKeys    int

        IsTruncated    bool
        Contents       []Key
        CommonPrefixes []string `xml:">Prefix"`
}

// The Key type represents an item stored in an SCS bucket.
type Key struct {
        Key          string
        LastModified string
        Size         int64
        // ETag gives the hex-encoded MD5 sum of the contents,
        // surrounded with double-quotes.
        ETag         string
        StorageClass string
        Owner        Owner
}

func (b *Bucket) List(prefix, delim, marker string, max int) (result *ListResp, err error) {
        params := map[string][]string{
                "prefix":    {prefix},
                "delimiter": {delim},
                "marker":    {marker},
        }
        if max != 0 {
                params["max-keys"] = []string{strconv.FormatInt(int64(max), 10)}
        }
        req := &request{
                bucket: b.Name,
                params: params,
        }
        result = &ListResp{}
        for attempt := attempts.Start(); attempt.Next(); {
                err = b.SCS.query(req, result)
                if !shouldRetry(err) {
                        break
                }
        }
        if err != nil {
                return nil, err
        }
        return result, nil
}

type ResponseType uint
const (
        kXmlBody = ResponseType(iota)
        kIgnoreResponse
        kJsonBody
        kMetaHeader
        kHeaderAndJsonBody
        kFunc
)
type ResponseFunc func (* http.Response) error

type request struct {
        method          string
        bucket          string
        path            string
        signpath        string
        params          url.Values
        headers         http.Header
        baseurl         string
        payload         io.Reader
        prepared        bool
        responsetype    ResponseType
}

func (req *request) CreateHTTPRequest() (hreq *http.Request, err error) {
        u, err := req.url(false)
        if err != nil {
                return nil, err
        }

        hreq = &http.Request{
                URL:        u,
                Method:     req.method,
                ProtoMajor: 1,
                ProtoMinor: 1,
                Close:      true,
                Header:     req.headers,
        }

        if v, ok := req.headers["Content-Length"]; ok {
                hreq.ContentLength, _ = strconv.ParseInt(v[0], 10, 64)
                delete(req.headers, "Content-Length")
        }
        if req.payload != nil {
                hreq.Body = ioutil.NopCloser(req.payload)
        }
        return
}
// URL returns a non-signed URL that allows retriving the
// object at path. It only works if the object is publicly
// readable (see SignedURL).
func (b *Bucket) URL(path string) string {
        req := &request{
                bucket: b.Name,
                path:   path,
        }
        err := b.SCS.prepare(req)
        if err != nil {
                panic(err)
        }
        u, err := req.url(true)
        if err != nil {
                panic(err)
        }
        u.RawQuery = ""
        return u.String()
}

// SignedURL returns a signed URL that allows anyone holding the URL
// to retrieve the object at path. The signature is valid until expires.
func (b *Bucket) SignedURL(path string, expires time.Time) string {
        req := &request{
                bucket: b.Name,
                path:   path,
                params: url.Values{"Expires": {strconv.FormatInt(expires.Unix(), 10)}},
        }
        err := b.SCS.prepare(req)
        if err != nil {
                panic(err)
        }
        u, err := req.url(true)
        if err != nil {
                panic(err)
        }
        return u.String()
}

// sohucsShouldEscape returns true if byte should be escaped
func sohucsShouldEscape(c byte) bool {
        return !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '~' || c == '.' || c == '/' || c == ':')
}

// sohucsEscape does uri escaping exactly as Sohucs does
func sohucsEscape(s string) string {
        hexCount := 0

        for i := 0; i < len(s); i++ {
                if sohucsShouldEscape(s[i]) {
                        hexCount++
                }
        }

        if hexCount == 0 {
                return s
        }

        t := make([]byte, len(s)+2*hexCount)
        j := 0
        for i := 0; i < len(s); i++ {
                if c := s[i]; sohucsShouldEscape(c) {
                        t[j] = '%'
                        t[j+1] = "0123456789ABCDEF"[c>>4]
                        t[j+2] = "0123456789ABCDEF"[c&15]
                        j += 3
                } else {
                        t[j] = s[i]
                        j++
                }
        }
        return string(t)
}

func (req *request) url(full bool) (*url.URL, error) {
        u, err := url.Parse(req.baseurl)
        if err != nil {
                return nil, fmt.Errorf("bad SCS endpoint URL %q: %v", req.baseurl, err)
        }

        u.Opaque = sohucsEscape(req.path)
        if full {
                u.Opaque = "//" + u.Host + u.Opaque
        }
        u.RawQuery = req.params.Encode()
        if strings.HasSuffix(u.RawQuery, "=") {
                u.RawQuery = strings.TrimSuffix(u.RawQuery, "=")
        }
        return u, nil
}

func (scs *SCS) query(req *request, resp interface{}) error {
        err := scs.prepare(req)
        if err == nil {
                // set redirect function
                scs.HTTPClient = func () (client *http.Client) {
                        client = http.DefaultClient
                        client.Transport = http.DefaultTransport
                        client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
                                return scs.redirectProc(request, via, req)
                        }
                        return
                }
                // run
                var httpResponse *http.Response
                httpResponse, err = scs.run(req, resp)
                if resp == nil && httpResponse != nil {
                        httpResponse.Body.Close()
                }
        }
        return err
}

func (scs *SCS) prepare(req *request) error {
        if !req.prepared {
                req.prepared = true
                if req.method == "" {
                        req.method = "GET"
                }
                params := make(url.Values)
                headers := make(http.Header)
                for k, v := range req.params {
                        params[k] = v
                }
                for k, v := range req.headers {
                        headers[k] = v
                }
                req.params = params
                req.headers = headers
                if !strings.HasPrefix(req.path, "/") {
                        req.path = "/" + req.path
                }
                req.signpath = req.path

                if req.bucket != "" {
                        req.baseurl = scs.Region.SCSBucketEndpoint
                        if req.baseurl == "" {
                                // Use the path method to address the bucket.
                                req.baseurl = scs.Region.SCSEndpoint
                                req.path = "/" + req.bucket + req.path
                        } else {
                                // Just in case, prevent injection.
                                if strings.IndexAny(req.bucket, "/:@") >= 0 {
                                        return fmt.Errorf("bad SCS bucket: %q", req.bucket)
                                }
                                req.baseurl = strings.Replace(req.baseurl, "${bucket}", req.bucket, -1)
                        }
                        req.signpath = "/" + req.bucket + req.signpath
                } else {
                        req.baseurl = scs.Region.SCSEndpoint
                }
        }

        // Always sign again as it's not clear how far the
        // server has handled a previous attempt.
        u, err := url.Parse(req.baseurl)
        if err != nil {
                return fmt.Errorf("bad SCS endpoint URL %q: %v", req.baseurl, err)
        }
        req.headers["Host"] = []string{u.Host}
        req.headers["Date"] = []string{time.Now().In(time.UTC).Format(time.RFC1123)}

        // signature
        if !strings.HasSuffix(req.signpath, "/") && u.Host == IAM.SCSEndpoint[len("http://"):] {
                req.signpath += "/"
        }
        sign(scs.Auth, req.method, sohucsEscape(req.signpath), req.params, req.headers)
        return nil
}

func (scs *SCS) run(req *request, resp interface{}) (*http.Response, error) {
        hreq, err := req.CreateHTTPRequest()
        if err != nil  {
                return nil, err
        }
        hresp, err := scs.HTTPClient().Do(hreq)
        if err != nil {
                return nil, err
        }
        // redirect
        if hresp.StatusCode >= 300 && hresp.StatusCode < 400 {
                new_request, err := scs.redirectProc2(req, hresp)
                if err != nil {
                        return nil, err
                }
                return scs.run(new_request, resp)
        }
        if hresp.StatusCode != 200 && hresp.StatusCode != 204 && hresp.StatusCode != 206 {
                defer hresp.Body.Close()
                return nil, buildError(hresp)
        }
        if resp != nil {
                switch req.responsetype {
                case kXmlBody:
                        if hresp.Body != nil {
                                err = xml.NewDecoder(hresp.Body).Decode(resp)
                        }
                        defer hresp.Body.Close()
                case kJsonBody:
                        if hresp.Body != nil {
                                err = json.NewDecoder(hresp.Body).Decode(resp)
                        }
                        defer hresp.Body.Close()
                case kMetaHeader:
                        err = GetMetaHeaderContent(hresp, resp)
                        defer hresp.Body.Close()
                case kHeaderAndJsonBody:
                        headResp, ok := resp.(ResponseWithHeader)
                        if !ok {
                                return nil, errors.New("response set kHeaderAndJsonBody mark" +
                                ", but provide a pointer not implemented ResponseWithHeader" +
                                " interface")
                        }
                        defer hresp.Body.Close()
                        GetHeaderContent(hresp, headResp)
                        err = json.NewDecoder(hresp.Body).Decode(resp)
                case kFunc:
                        err = resp.(ResponseFunc)(hresp)
                        defer hresp.Body.Close()
                case kIgnoreResponse:
                default :
                        err = errors.New("unknow required return type")
                }
        }
        return hresp, err
}

// Error represents an error in an operation with SCS.
type Error struct {
        StatusCode int    // HTTP status code (200, 403, ...)
        Code       string //
        Message    string // The human-oriented error message
        BucketName string
        RequestId  string
        HostId     string
}

func (e *Error) Error() string {
        return e.Message
}

func buildError(r *http.Response) error {
        err := Error{}
        xml.NewDecoder(r.Body).Decode(&err)
        r.Body.Close()
        err.StatusCode = r.StatusCode
        if err.Message == "" {
                err.Message = r.Status
        }
        return &err
}

func shouldRetry(err error) bool {
        if err == nil {
                return false
        }
        switch err {
        case io.ErrUnexpectedEOF, io.EOF:
                return true
        }
        switch e := err.(type) {
        case *net.DNSError:
                return true
        case *net.OpError:
                switch e.Op {
                case "read", "write":
                        return true
                }
        case *Error:
                switch e.Code {
                case "InternalError", "NoSuchUpload", "NoSuchBucket":
                        return true
                }
        }
        return false
}

func hasCode(err error, code string) bool {
        scserr, ok := err.(*Error)
        return ok && scserr.Code == code
}

type httpRequestDescriptor http.Request
func (req httpRequestDescriptor) String() string {
        return fmt.Sprintln("Method=", req.Method) +
        fmt.Sprintln("URL=", req.URL) +
        fmt.Sprintln("Proto=", req.Proto) +
        fmt.Sprintln("ProtoMajor=", req.ProtoMajor) +
        fmt.Sprintln("ProtoMinor=", req.ProtoMinor) +
        fmt.Sprintln("Header=", req.Header) +
        fmt.Sprintln("Body=", req.Body) +
        fmt.Sprintln("ContentLength=", req.ContentLength) +
        fmt.Sprintln("TransferEncoding=", req.TransferEncoding) +
        fmt.Sprintln("Close=", req.Close) +
        fmt.Sprintln("Host=", req.Host) +
        fmt.Sprintln("Form=", req.Form) +
        fmt.Sprintln("PostForm=", req.PostForm) +
        fmt.Sprintln("MultipartForm=", req.MultipartForm) +
        fmt.Sprintln("Trailer=", req.Trailer) +
        fmt.Sprintln("RemoteAddr=", req.RemoteAddr) +
        fmt.Sprintln("RequestURI=", req.RequestURI);
}

// meta data related functions
type GetMetaResp map[string]string

func (bucket *Bucket) GetMeta(object string) (result *GetMetaResp, err error){
        // prepare
        req := &request {
                bucket: bucket.Name,
                method: "HEAD",
                path: object,
                responsetype: kMetaHeader,
        }
        req.headers = make(http.Header)
        req.headers.Add("x-scs-version-id", "")
        req.params = make(url.Values)
        // query
        result = &GetMetaResp{}
        for attempt := attempts.Start(); attempt.Next(); {
                err = bucket.SCS.query(req, map[string]string(*result))
                if !shouldRetry(err) {
                        break
                }
        }
        // return
        if err != nil {
                return nil, err
        }
        return result, nil
}

type SetMetaResp map[string]string
func (bucket *Bucket) SetMeta(object string, keyvalues map[string]string) (result *SetMetaResp, err error){
        // prepare
        req := &request {
                bucket: bucket.Name,
                method: "PUT",
                path: object,
                responsetype: kIgnoreResponse,
        }
        req.headers = make(http.Header)
        req.headers.Add("x-scs-version-id", "")
        req.params = make(url.Values)
        req.params.Add("metadata", "")
        reqbody := FormatMetaInfo(keyvalues)
        req.payload = ioutil.NopCloser(strings.NewReader(
                reqbody))
        // query
        result = &SetMetaResp{}
        for attempt := attempts.Start(); attempt.Next(); {
                err = bucket.SCS.query(req, result)
                if !shouldRetry(err) {
                        break
                }
        }
        if err != nil {
                return nil,err
        }
        return result, nil
}

func GetHeaderContent(httpResponse *http.Response, response ResponseWithHeader) {
        if response == nil {
                return
        }
        respHeader := response.GetHeader();
        for key, values := range map[string][]string(httpResponse.Header) {
                str_tmp := ""
                for _, value := range values {
                        str_tmp = str_tmp + value
                }
                respHeader[strings.ToLower(key)] = str_tmp
        }
        return
}

func GetMetaHeaderContent(httpResponse *http.Response, response interface{}) (err error) {

        if response == nil {
                return nil
        }
        resp, ok := response.(map[string]string);
        if !ok {
                return errors.New("input paramater type error")
        }

        for key, values := range map[string][]string(httpResponse.Header) {
                // check
                if !IsMatchMetaInfoFormat(key) {
                        continue
                }
                str_tmp := ""
                for _, value := range values {
                        str_tmp = str_tmp + value
                }
                resp[key] = str_tmp
        }
        return nil
}

func (scs SCS) redirectProc(hreq *http.Request, via []*http.Request, originRequest *request) (err error) {
        scs.Region = Region{"tmp", hreq.URL.Scheme + "://" + hreq.URL.Host, ""}

        // scs.Region = sohucs.Regions[strings.TrimSuffix(hreq.URL.Host, ".scs.sohucs.com")]
        if originRequest.bucket != "" { //  && strings.HasSuffix(originRequest.baseurl, ".scs.sohucs.com") {
                originRequest.path = originRequest.path[strings.IndexAny(originRequest.path[1:], "/") + 1:]
        }
        // remake request
        originRequest.prepared = false
        originRequest.headers.Del("Authorization")
        err = scs.prepare(originRequest)
        if err != nil {
                return
        }
        hreq_tmp, err := originRequest.CreateHTTPRequest()
        *hreq = *hreq_tmp
        if err != nil {
                return
        }
        return nil
}

func (scs *SCS)redirectProc2(req *request, hresp *http.Response) (new_req *request, err error) {
        switch hresp.StatusCode {
        case 307:
                // get new location
                locations, ok := hresp.Header["Location"]
                if !ok || len(locations) == 0{
                        return nil, errors.New("redirect, but could not find new location")
                }
                location_url, err := url.Parse(locations[0])

                // reset location
                new_req = req
                req.payload = bytes.NewReader(scs.tmp_body)
                req.headers.Add("Content-Length", strconv.FormatInt(int64(len(scs.tmp_body)), 10))
                scs.Region = Region{"tmp", location_url.Scheme + "://" + location_url.Host, ""}

                if new_req.bucket != "" {
                        new_req.path = new_req.path[strings.IndexAny(new_req.path[1:], "/") + 1:]
                }

                // remake request
                new_req.prepared = false
                new_req.headers.Del("Authorization")
                err = scs.prepare(new_req)
                if err != nil {
                        return nil, err
                }
                return new_req, nil
        default:
                return nil, errors.New("unknow redirect")
        }
}

type makeBucketConfig struct {
        XMLName     xml.Name    `xml:"CreateBucketConfiguration"`
        Region      string      `xml:"LocationConstraint"`
}

func (scs *SCS)MkBucket(bucketName string) (err error) {
        // create request
        req := &request {
                bucket: bucketName,
                method: "PUT",
        }
        bucketConfig := makeBucketConfig {
                Region: scs.Region.SCSEndpoint[len("http://"):],
        }
        innerBuffer := []byte{}
        buffer := bytes.NewBuffer(innerBuffer)
        err = xml.NewEncoder(buffer).Encode(&bucketConfig)
        if err != nil {
                return
        }
        scs.tmp_body= buffer.Bytes()

        req.headers = make(http.Header)
        req.headers.Add("Content-Length", strconv.FormatInt(int64(len(scs.tmp_body)), 10))

        // query
        var oldRegion Region = scs.Region
        scs.Region = BJCNC
        for attempt := attempts.Start(); attempt.Next(); {
                req.headers = make(http.Header)
                req.headers.Add("Content-Length", strconv.FormatInt(int64(len(scs.tmp_body)), 10))
                req.payload = bytes.NewReader(scs.tmp_body)
                err = scs.query(req, nil)
                if !shouldRetry(err) {
                        break
                }
        }
        scs.Region = oldRegion
        return
}

type ResponseWithHeader interface{
        GetHeader() map[string]string
}

type BucketAuthorityResponse interface {
        ParseBucketAuthorityResponse (io.Reader) (ok bool)
}
type BucketEnableAuthorityResponse struct {
        Version         string
        Statement       []EnableAuthorityStatement
}
type BucketDisableAuthorityResponse struct {
        Version         string
        Statement       []DisableAuthorityStatement
}
func (resp *BucketEnableAuthorityResponse)ParseBucketAuthorityResponse (
body io.Reader) (bool) {
        json.NewDecoder(body).Decode(resp)
        if len(resp.Statement) == 0 {
                return false
        }
        for _, statement := range resp.Statement {
                if len(statement.Action) == 0 && statement.Principal == "" {
                        return false
                }
        }
        return true
}

func (resp *BucketDisableAuthorityResponse)ParseBucketAuthorityResponse (
body io.Reader) (bool) {
        json.NewDecoder(body).Decode(resp)
        if len(resp.Statement) == 0 {
                return false
        }
        emptyStruct := PrincipalStruct{}
        for _, statement := range resp.Statement {
                if statement.Action == "" && statement.Principal == emptyStruct {
                        return false
                }
        }
        return true
}

type EnableAuthorityStatement struct {
        Effect          string
        Action          []string        `json:"Action"`
        Principal       string          `json:"Principal"`
        Resource        []string
}
type DisableAuthorityStatement struct {
        Effect          string
        Action          string          `json:"Action"`
        Principal       PrincipalStruct `json:"Principal"`
        Resource        []string
}
type PrincipalStruct struct {
        SOHUCS          string
}

func ParseBucketAuthorityResponse(hrep * http.Response)(
rep BucketAuthorityResponse, err error) {
        buffer := bytes.NewBuffer([]byte{})
        rep = &BucketEnableAuthorityResponse{}
        ok := rep.ParseBucketAuthorityResponse(io.TeeReader(hrep.Body, buffer))
        if ok {
                return
        }

        rep = &BucketDisableAuthorityResponse{}
        ok  = rep.ParseBucketAuthorityResponse(buffer)
        if ok {
                return
        }
        return nil, errors.New("could not parse the http response body")
}

func (bucket *Bucket) GetAuthority() (response string, err error) {
        // query
        req := &request{
                method  : "GET",
                params  : map[string][]string {"policy" : {""}},
                bucket  : bucket.Name,
                responsetype : kFunc,
        }
        var resp BucketAuthorityResponse
        respFunc    := ResponseFunc( func (hrep * http.Response) (err error) {
                resp, err = ParseBucketAuthorityResponse(hrep)
                return
        })
        for attempt := attempts.Start(); attempt.Next(); {
                err = bucket.SCS.query(req, respFunc)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                // judge for weaher the bucket is readable for everyone
                if err != nil {
                        return
                }
                switch resp.(type) {
                case *BucketEnableAuthorityResponse:
                        return "read", nil
                case *BucketDisableAuthorityResponse:
                        return "unread", nil
                default:
                        return "", errors.New("unknow type message return")
                }
        }
        return
}

type SetAuthorityResponse struct{
        Data		setAuthorityData	`json:"data"`
        ErrorCode	string				`json:"errorCode"`
        ErrorMsg	string				`json:"errorMsg"`
        HasError	bool				`json:"hasError"`
}
type setAuthorityData	struct {
        Flag		bool				`json:"flag"`
}

func (bucket *Bucket) SetAuthority(enable bool) (err error) {
        EnableBody  := "{\"Version\": \"2014-03-26\",\"Statement\":[{\"Effect\": \"Allow\",\"Action\": [\"scs:listObjects\",\"scs:listVersions\",\"scs:listMultipartUploads\",\"scs:listParts\",\"scs:getObject\",\"scs:getObjectMetadata\"],\"Principal\": \"*\",\"Resource\": [\"srn:sohucs:scs:::$BUCKET_NAME$/*\",\"srn:sohucs:scs:::$BUCKET_NAME$\"]}]}"
        DisableBody := "{\"Version\":\"2014-03-26\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"*\",\"Principal\":{\"SOHUCS\":\"$BUCKET_OWNER_SRN$\"},\"Resource\":[\"srn:sohucs:scs:::$BUCKET_NAME$/*\",\"srn:sohucs:scs:::$BUCKET_NAME$\"]}]}"

        headers := map[string][]string {
                "Content-Type"  : {"application/x-www-form-urlencoded"},
        }
        // var path string
        var body string
        if enable {
                body = strings.Replace(EnableBody, "$BUCKET_NAME$", bucket.Name, -1)
        } else {
                body = strings.Replace(DisableBody, "$BUCKET_NAME$", bucket.Name, -1)
                usrInfo, err := bucket.SCS.GetUserInfo()
                if err != nil {
                        return err
                }
                body = strings.Replace(body, "$BUCKET_OWNER_SRN$", usrInfo.Name, -1)
        }
        // query
        req := &request{
                method  : "PUT",
                params  : map[string][]string { "policy" : {""} },
                headers : headers,
                responsetype : kJsonBody,
                payload : bytes.NewBufferString(body),
                bucket  : bucket.Name,
        }
        for attempt := attempts.Start(); attempt.Next(); {
                err = bucket.SCS.query(req, nil)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                return
        }
        return

}

type UsrInfo struct {
        Account		xml.Name		`xml:"Account"`
        Name		string
        Password	string
        CreateTime	string
        Mail		string
        Salt		string
        Phone		string
        Status		string
}
func (scs *SCS) GetUserInfo() (usrInfo *UsrInfo, err error) {
        originRegion := scs.Region
        scs.Region = IAM
        req := &request {
                method	:	"GET",
                path	:	"/account",
        }
        usrInfo = &UsrInfo{}
        for attempt := attempts.Start(); attempt.Next(); {
                err = scs.query(req, usrInfo)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                scs.Region = originRegion
                return
        }
        scs.Region = originRegion
        return
}

type ListDistributionResponse struct {
        HasError    bool                `json:"hasError"`
        Data        []Distribution      `json:"data"`
}

func (list ListDistributionResponse) String() (str string) {
        str = "HasError : " + strconv.FormatBool(list.HasError) + "\nData : [\n"
        for _, distribution := range list.Data {
                str += distribution.String()
        }
        str += "\n]"
        return
}

type Distribution struct {
        AccountId   string      `json:"account_id"`
        Cname       string      `json:"cname"`
        DomainName  string      `json:"domain_name"`
        InternalId  string      `json:"internal_id"`
        Enable      bool        `json:"enabled"`
}

func (distribution Distribution) String() string{
        return "AccountId : " + distribution.AccountId + ", Cname : " + distribution.Cname + ", DomainName : " + distribution.DomainName + ", InternalId : " + distribution.InternalId + ", Enable : " + strconv.FormatBool(distribution.Enable)
}

func (scs *SCS) GetDistributionList() (distribution_list []Distribution, err error) {
        UsrInfo, err    := scs.GetUserInfo()
        if err != nil {
                return nil, err
        }

        params  := map[string][]string {
                "view"          :   {"brief"},
                "accountId"     :   {UsrInfo.Name},
        }

        req := &request {
                method  :   "GET",
                path    :   "cdn/distribution",
                params  :   params,
                responsetype    :   kJsonBody,
        }

        response    := &ListDistributionResponse{}
        for attempt := attempts.Start(); attempt.Next(); {
                err = scs.query(req, response)
                if shouldRetry(err) && attempt.HasNext() {
                        continue
                }
                if err != nil {
                        return nil, err
                }
                if response.HasError {
                        return response.Data, errors.New("server return internal error")
                } else {
                        return response.Data, nil
                }
        }
        return nil, errors.New("query server failed")
}

func (scs *SCS) GetDistributionFromCName(cname string) (Distribution, error) {
        distribution_list, err := scs.GetDistributionList()
        if err != nil {
                return Distribution{}, err
        }

        for _, distribution := range distribution_list {
                if distribution.Cname != cname {
                        continue
                }
                if distribution.Enable {
                        return distribution, nil
                } else {
                        return distribution, errors.New("this distribution " + cname + " is disabled")
                }
        }
        return Distribution{}, errors.New("no distribution " + cname + " is existed");
}
