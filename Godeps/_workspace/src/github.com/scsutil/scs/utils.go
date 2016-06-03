package scs

import (
        "strings"
        "regexp"
)

var MetaInfoItem = [...]string {
        "^cache-control$",
        "^content-disposition$",
        "^content-type$",
        "^expires$",
        "^content-encodings$",
        "^x-scs-meta-.*",
}

func IsMatchMetaInfoFormat(str string) bool {
        for _, regexpstr := range MetaInfoItem {
                if match, err := regexp.Match(regexpstr, []byte(strings.ToLower(str)));
                match && err == nil {
                        return true
                }
        }
        return false
}

func FormatMetaInfo(keyvalue map[string]string) (metainfo string) {
        metainfo = "{\n";
        for key, value := range keyvalue {
                metainfo += key + ":\"" + value + "\",\n"
        }
        metainfo += "}\n"
        return
}