package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"time"
)

// S3 provides a wrapper around your S3 credentials. It carries no other internal state
// and can be copied freely.
type S3 struct {
	bucket   string
	accessId string
	secret   string
}

// NewS3 allocates a new S3 with the provided credentials.
func NewS3(bucket, accessId, secret string) *S3 {
	return &S3{
		bucket:   bucket,
		accessId: accessId,
		secret:   secret,
	}
}

func (s3 *S3) signRequest(req *http.Request) {
	amzHeaders := ""
	resource := "/" + s3.bucket + req.URL.Path

	/* Ugh, AWS requires us to order the parameters in a specific ordering for
	 * signing. Makes sense, but is annoying because a map does not have a defined
	 * ordering (and basically returns elements in a random order) -- so we have
	 * to sort by hand */
	query := req.URL.Query()
	if len(query) > 0 {
		keys := []string{}

		for k := range query {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		parts := []string{}

		for _, key := range keys {
			vals := query[key]

			for _, val := range vals {
				if val == "" {
					parts = append(parts, url.QueryEscape(key))

				} else {
					part := fmt.Sprintf("%s=%s", url.QueryEscape(key), url.QueryEscape(val))
					parts = append(parts, part)
				}
			}
		}

		req.URL.RawQuery = strings.Join(parts, "&")
	}

	if req.URL.RawQuery != "" {
		resource += "?" + req.URL.RawQuery
	}

	if req.Header.Get("Date") == "" {
		req.Header.Set("Date", time.Now().Format(time.RFC1123))
	}

	authStr := strings.Join([]string{
		strings.TrimSpace(req.Method),
		req.Header.Get("Content-MD5"),
		req.Header.Get("Content-Type"),
		req.Header.Get("Date"),
		amzHeaders + resource,
	}, "\n")

	h := hmac.New(sha1.New, []byte(s3.secret))
	h.Write([]byte(authStr))

	h64 := base64.StdEncoding.EncodeToString(h.Sum(nil))
	auth := "AWS" + " " + s3.accessId + ":" + h64
	req.Header.Set("Authorization", auth)
}

func (s3 *S3) resource(path string, values url.Values) string {
	tmp := fmt.Sprintf("http://%s.s3.amazonaws.com/%s", s3.bucket, path)

	if values != nil {
		tmp += "?" + values.Encode()
	}

	return tmp
}

func (s3 *S3) putMultipart(r io.Reader, size int64, path string, contentType string) (er error) {
	mp, er := s3.StartMultipart(path)
	if er != nil {
		return er
	}
	defer func() {
		if er != nil {
			mp.Abort()
		}
	}()

	var chunkSize int64 = 7 * 1024 * 1024
	chunk := bytes.NewBuffer(make([]byte, chunkSize))
	md5hash := md5.New()
	remaining := size

	for ; remaining > 0; remaining -= chunkSize {
		chunk.Reset()
		md5hash.Reset()

		if remaining < chunkSize {
			chunkSize = remaining
		}

		wr := io.MultiWriter(chunk, md5hash)

		if _, er := io.CopyN(wr, r, chunkSize); er != nil {
			return er
		}

		if er := mp.AddPart(chunk, chunkSize, md5hash.Sum(nil)); er != nil {
			return er
		}
	}

	return mp.Complete(contentType)
}

// Put uploads content to S3. The length of r must be passed as size. md5sum optionally contains
// the MD5 hash of the content for end-to-end integrity checking; if omitted no checking is done.
// contentType optionally contains the MIME type to send to S3 as the Content-Type header; when
// files are later accessed, S3 will echo back this in their response headers.
//
// If the passed size exceeds 3GB, the multipart API is used, otherwise the single-request API is used.
// It should be noted that the multipart API uploads in 7MB segments and computes checksums of each
// one -- it does NOT use the passed md5sum, so don't bother with it if you're uploading huge files.
func (s3 *S3) Put(r io.Reader, size int64, path string, md5sum []byte, contentType string) error {
	if size > 3*1024*1024*1024 {
		return s3.putMultipart(r, size, path, contentType)
	}

	req, er := http.NewRequest("PUT", s3.resource(path, nil), r)
	if er != nil {
		return er
	}

	if md5sum != nil {
		md5value := base64.StdEncoding.EncodeToString(md5sum)
		req.Header.Set("Content-MD5", md5value)
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", size))
	req.Header.Set("Host", req.URL.Host)
	req.ContentLength = size

	s3.signRequest(req)

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return er
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return wrapError(resp)
	}

	return nil
}

// Get fetches content from S3, returning both a ReadCloser for the data and the HTTP headers
// returned by S3. You can use the headers to extract the Content-Type that the data was sent
// with.
func (s3 *S3) Get(path string) (io.ReadCloser, http.Header, error) {
	req, er := http.NewRequest("GET", s3.resource(path, nil), nil)
	if er != nil {
		return nil, http.Header{}, er
	}

	s3.signRequest(req)

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return nil, http.Header{}, er
	}

	if resp.StatusCode != 200 {
		return nil, http.Header{}, wrapError(resp)
	}

	return resp.Body, resp.Header, nil
}

// Head is similar to Get, but returns only the response headers. The response body is not
// transferred across the network. This is useful for checking if a file exists remotely,
// and what headers it was configured with.
func (s3 *S3) Head(path string) (http.Header, error) {
	req, er := http.NewRequest("HEAD", s3.resource(path, nil), nil)
	if er != nil {
		return http.Header{}, er
	}

	s3.signRequest(req)

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return http.Header{}, er
	}

	if resp.StatusCode != 200 {
		return http.Header{}, wrapError(resp)
	}

	return resp.Header, nil
}

// Test attempts to write and read back a single, short file from S3. It is intended to be
// used to test runtime configuration to fail quickly when credentials are invalid.
func (s3 *S3) Test() error {
	testString := fmt.Sprintf("roundtrip-test-%d", rand.Int())
	testReader := strings.NewReader(testString)

	if er := s3.Put(testReader, int64(testReader.Len()), "writetest", nil, "text/x-empty"); er != nil {
		return er
	}

	actualReader, header, er := s3.Get("writetest")
	if er != nil {
		return er
	}
	defer actualReader.Close()

	actualBytes, er := ioutil.ReadAll(actualReader)
	if er != nil {
		return er
	}

	if string(actualBytes) != testString {
		return fmt.Errorf("String read back from S3 was different than what we put there.")
	}

	if header.Get("Content-Type") != "text/x-empty" {
		return fmt.Errorf("Content served back from S3 had a different Content-Type than what we put there")
	}

	return nil
}

// StartMultipart initiates a multipart upload.
func (s3 *S3) StartMultipart(path string) (*S3Multipart, error) {
	req, er := http.NewRequest("POST", s3.resource(path, nil)+"?uploads", nil)
	if er != nil {
		return nil, er
	}

	req.Header.Set("Host", req.URL.Host)

	s3.signRequest(req)

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return nil, er
	}
	defer resp.Body.Close()

	xmlBytes, er := ioutil.ReadAll(resp.Body)
	if er != nil {
		return nil, er
	}

	if resp.StatusCode != 200 {
		return nil, wrapError(resp)
	}

	var xmlResp s3multipartResp
	if er := xml.Unmarshal(xmlBytes, &xmlResp); er != nil {
		return nil, er
	}

	mp := &S3Multipart{
		uploadId: xmlResp.UploadId,
		key:      xmlResp.Key,
		s3:       s3,
	}

	runtime.SetFinalizer(mp, func(mp *S3Multipart) {
		mp.Abort()
	})

	return mp, nil
}
