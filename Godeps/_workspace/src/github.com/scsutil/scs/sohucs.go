package scs

type Auth struct{
        AccessKey, SecretKey string
}

type Region struct {
        Name string
        SCSEndpoint string
        SCSBucketEndpoint string
}

var BJCNC = Region{
        "bjcnc",
        "http://bjcnc.scs.sohucs.com",
        "",
}

var BJCTC = Region{
        "bjctc",
        "http://bjctc.scs.sohucs.com",
        "",
}

var SHCTC = Region{
        "shctc",
        "http://shctc.scs.sohucs.com",
        "",
}

var GZCTC = Region{
        "gzctc",
        "http://gzctc.scs.sohucs.com",
        "",
}

var BJCNCINTERNAL = Region{
        "bjcnc-internal",
        "http://bjcnc.scs-in.sohucs.com",
        "",
}

var BJCTCINTERNAL = Region{
        "bjctc-internal",
        "http://bjctc.scs-in.sohucs.com",
        "",
}

var SHCTCINTERNAL = Region{
        "shctc-internal",
        "http://shctc.scs-in.sohucs.com",
        "",
}

var GZCTCINTERNAL = Region{
        "gzctc-internal",
        "http://gzctc.scs-in.sohucs.com",
        "",
}

var IAM = Region{
        "iam",
        "http://bjctc.iam.sohucs.com",
        "",
}

var Regions = map[string]Region{
        BJCNC.Name: BJCNC,
        BJCTC.Name: BJCTC,
        SHCTC.Name: SHCTC,
        GZCTC.Name: GZCTC,
        BJCNCINTERNAL.Name: BJCNCINTERNAL,
        BJCTCINTERNAL.Name: BJCTCINTERNAL,
        SHCTCINTERNAL.Name: SHCTCINTERNAL,
        GZCTCINTERNAL.Name: GZCTCINTERNAL,
}
