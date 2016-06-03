package scs

import (
        "sort"
        "strings"
        "crypto/sha1"
        "crypto/hmac"
        "encoding/base64"
)

var b64 = base64.StdEncoding

var scsParamsToSign = map[string]bool{
        "acl":                          true,
        "delete":                       false,
        "location":                     true,
        "logging":                      false,
        "notification":                 false,
        "policy":                       false,
        "requestPayment":               false,
        "response-content-type":        false,
        "response-content-language":    false,
        "response-expires":             false,
        "response-cache-control":       false,
        "response-content-disposition": false,
        "response-content-encoding":    false,
}

func sign(auth Auth, method, canonicalPath string, params, headers map[string][]string) {
        var md5, ctype, date, xsohucs string
        var xsohucsDate bool
        var sarray []string

        if auth.SecretKey == ""{
                return
        }
        for k, v := range headers {
                k = strings.ToLower(k)
                switch k {
                case "content-md5":
                        md5 = v[0]
                case "content-type":
                        ctype = v[0]
                case "date":
                        if !xsohucsDate {
                                date = v[0]
                        }
                default:
                        if strings.HasPrefix(k, "x-scs-") {
                                vall := strings.Join(v, ",")
                                sarray = append(sarray, k+":"+vall)
                                if k == "x-scs-date" {
                                        xsohucsDate = true
                                        date = ""
                                }
                        }
                }
        }

        if len(sarray) > 0 {
                sort.StringSlice(sarray).Sort()
                xsohucs = strings.Join(sarray, "\n") + "\n"
        }

        expires := false
        if v, ok := params["Expires"]; ok {
                // Query string request authentication alternative.
                expires = true
                date = v[0]
                params["SOHUCSAccessKeyId"] = []string{auth.AccessKey}
        }

        sarray = sarray[0:0]
        for k, v := range params {
                if scsParamsToSign[k] {
                        for _, vi := range v {
                                if vi == "" {
                                        sarray = append(sarray, k)
                                } else {
                                        // "When signing you do not encode these values."
                                        sarray = append(sarray, k+"="+vi)
                                }
                        }
                }
        }
        if len(sarray) > 0 {
                sort.StringSlice(sarray).Sort()
                canonicalPath = canonicalPath + "?" + strings.Join(sarray, "&")
        }

        payload := strings.ToLower(method) + "\n" + md5 + "\n" + ctype + "\n" + date + "\n" + xsohucs + canonicalPath
        hash := hmac.New(sha1.New, []byte(auth.SecretKey))
        hash.Write([]byte(payload))
        signature := make([]byte, b64.EncodedLen(hash.Size()))
        b64.Encode(signature, hash.Sum(nil))

        if expires {
                params["Signature"] = []string{string(signature)}
        } else {
                headers["Authorization"] = []string{"" + auth.AccessKey + ":" + string(signature)}
        }
}