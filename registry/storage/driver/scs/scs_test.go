// +build include_scs

package scs

import (
        "os"
        "testing"

        scsutil "github.com/scsutil/scs"

        ctx "github.com/docker/distribution/context"
        storagedriver "github.com/docker/distribution/registry/storage/driver"
        "github.com/docker/distribution/registry/storage/driver/testsuites"

        "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var scsDriverConstructor func() (*Driver, error)
var skipscs func() string

func init() {
        accesskey := os.Getenv("REGISTRY_STORAGE_SCS_ACCESSKEY")
        secretkey := os.Getenv("REGISTRY_STORAGE_SCS_SECRETKEY")
        region := os.Getenv("REGISTRY_STORAGE_SCS_REGION")
        bucket := os.Getenv("REGISTRY_STORAGE_SCS_BUCKET")

        scsDriverConstructor = func() (*Driver, error) {

                parameters := DriverParameters{

                        Auth:   scsutil.Auth{
                                AccessKey:  accessKey,
                                SecretKey:  secretKey,
                        },
                        Region: scsutil.Regions[region],
                        Bucket: bucket,
                        ChunkSize: minChunkSize,
                }

                return New(parameters)
        }

        // Skip scs storage driver tests if environment variable parameters are not provided
        skipscs = func() string {
                if accesskey == "" || secretkey == "" || region == "" || bucket == "" {
                        return "Must set REGISTRY_STORAGE_SCS_ACCESSKEY, REGISTRY_STORAGE_SCS_SECRETKEY, REGISTRY_STORAGE_SCS_REGION and REGISTRY_STORAGE_SCS_BUCKET to run scs tests"
                }
                return ""
        }

        testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
                return scsDriverConstructor()
        }, skipscs)
}

func TestEmptyRootList(t *testing.T) {
        if skipscs() != "" {
                t.Skip(skipscs())
        }

        testDriver, err := scsDriverConstructor()
        if err != nil {
                t.Fatalf("unexpected error creating test driver: %v", err)
        }

        filename := "/test"
        contents := []byte("contents")
        ctx := ctx.Background()
        err = testDriver.PutContent(ctx, filename, contents)
        if err != nil {
                t.Fatalf("unexpected error creating content: %v", err)
        }
        defer testDriver.Delete(ctx, filename)

        keys, err := testDriver.List(ctx, "/")
        for _, path := range keys {
                if !storagedriver.PathRegexp.MatchString(path) {
                        t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
                }
        }
}








