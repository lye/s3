package s3

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

var accessId = strings.TrimSpace(os.ExpandEnv("$S3_ACCESS_ID"))
var secretKey = strings.TrimSpace(os.ExpandEnv("$S3_SECRET_KEY"))
var bucket = strings.TrimSpace(os.ExpandEnv("$S3_BUCKET"))

func getS3(t *testing.T) *S3 {
	if accessId == "" {
		t.Fatalf("Must set S3_ACCESS_ID in ENV")
	}

	if secretKey == "" {
		t.Fatalf("Must set S3_SECRET_KEY in ENV")
	}

	if bucket == "" {
		t.Fatalf("Must set S3_BUCKET in ENV")
	}

	return NewS3(bucket, accessId, secretKey)
}

func TestS3(t *testing.T) {
	s3 := getS3(t)

	if er := s3.Test(); er != nil {
		t.Fatal(er)
	}
}

func TestS3RoundTrip(t *testing.T) {
	s3 := getS3(t)

	testStr := "hello"
	testBuf := bytes.NewBuffer([]byte(testStr))
	testPath := ".hellopath"

	if er := s3.Put(testBuf, int64(testBuf.Len()), testPath, nil, ""); er != nil {
		t.Fatal(er)
	}

	r, _, er := s3.Get(testPath)
	if er != nil {
		t.Fatal(er)
	}
	defer r.Close()

	retBytes, er := ioutil.ReadAll(r)
	if er != nil {
		t.Fatal(er)
	}

	if string(retBytes) != testStr {
		t.Errorf("RTT failure: %#v != %#v", string(retBytes), testStr)
	}
}

func TestS3Multipart(t *testing.T) {
	s3 := getS3(t)

	testStr := "hello"
	testBuf := bytes.NewBuffer([]byte(testStr))
	testPath := ".hellopath"

	mp, er := s3.StartMultipart(testPath)
	if er != nil {
		t.Fatal(er)
	}
	defer mp.Abort()

	if er := mp.AddPart(testBuf, int64(testBuf.Len()), nil); er != nil {
		t.Fatal(er)
	}

	if er := mp.Complete(""); er != nil {
		t.Fatal(er)
	}

	r, _, er := s3.Get(testPath)
	if er != nil {
		t.Fatal(er)
	}
	defer r.Close()

	retBytes, er := ioutil.ReadAll(r)
	if er != nil {
		t.Fatal(er)
	}

	if string(retBytes) != testStr {
		t.Errorf("RTT failure: %#v != %#v", string(retBytes), testStr)
	}
}

func TestS3Multipart2(t *testing.T) {
	s3 := getS3(t)

	testStr := "HELLO"
	testBuf := bytes.NewBuffer([]byte(testStr))
	testPath := ".hellopath"

	if er := s3.putMultipart(testBuf, int64(testBuf.Len()), testPath, ""); er != nil {
		t.Fatal(er)
	}

	r, _, er := s3.Get(testPath)
	if er != nil {
		t.Fatal(er)
	}
	defer r.Close()

	retBytes, er := ioutil.ReadAll(r)
	if er != nil {
		t.Fatal(er)
	}

	if string(retBytes) != testStr {
		t.Errorf("RTT failure: %#v != %#v", string(retBytes), testStr)
	}
}
