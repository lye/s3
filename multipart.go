package s3

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
)

// S3Multipart tracks the state of a multipart upload, and provides an interface for streaming
// data to S3 in chunks. All methods on S3Multipart are mutually locked to ensure state doesn't
// become corrupt.
type S3Multipart struct {
	etags     []string
	uploadId  string
	key       string
	completed bool
	s3        *S3
	lock      sync.Mutex
}

type s3multipartResp struct {
	XMLName  string `xml:InitiateMultipartUploadResult`
	Bucket   string
	Key      string
	UploadId string
}

// AddPart uploads the contents of r to S3. The number of bytes that r will read must be passed
// as size (otherwise the request cannot be signed). Optionally, you can pass the md5sum of the
// bytes which will be verified on S3's end; if md5sum is nil no end-to-end integrity checking
// is performed. As per S3's API, size must always exceed 5MB (1024 * 1024 * 5) bytes, except
// for the last part. This is not enforced locally.
func (mp *S3Multipart) AddPart(r io.Reader, size int64, md5sum []byte) error {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	if mp.completed {
		return fmt.Errorf("s3: cannot call AddPart on an aborted multipart request")
	}

	values := url.Values{}
	values.Set("uploadId", mp.uploadId)
	values.Set("partNumber", fmt.Sprintf("%d", len(mp.etags)+1))

	req, er := http.NewRequest("PUT", mp.s3.resource(mp.key, values), r)
	if er != nil {
		return er
	}

	if md5sum != nil {
		md5value := base64.StdEncoding.EncodeToString(md5sum)
		req.Header.Set("Content-MD5", md5value)
	}

	req.Header.Set("Content-Length", fmt.Sprintf("%d", size))
	req.Header.Set("Host", req.URL.Host)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = size

	mp.s3.signRequest(req)

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return er
	}
	defer resp.Body.Close()

	body, er := ioutil.ReadAll(resp.Body)
	if er != nil {
		return er
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("s3: AddPart returned an error (HTTP %d)\n%s", resp.StatusCode, string(body))
	}

	mp.etags = append(mp.etags, resp.Header.Get("ETag"))
	return nil
}

// Complete finalizes the upload, and should be called after all parts have been added.
func (mp *S3Multipart) Complete(contentType string) error {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	if mp.completed {
		return fmt.Errorf("s3: cannot call Complete on an aborted multipart request")
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	/* ghetto request body generation, bleh */
	xmlBody := ""
	for idx, etag := range mp.etags {
		xmlBody += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", idx+1, etag)
	}
	xmlBody = "<CompleteMultipartUpload>" + xmlBody + "</CompleteMultipartUpload>"

	r := bytes.NewBuffer([]byte(xmlBody))

	values := url.Values{}
	values.Set("uploadId", mp.uploadId)

	req, er := http.NewRequest("POST", mp.s3.resource(mp.key, values), r)
	if er != nil {
		return er
	}

	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(xmlBody)))
	req.Header.Set("Host", req.URL.Host)
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = int64(len(xmlBody))

	mp.s3.signRequest(req)

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return er
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("s3: Complete returned an error (HTTP %d)", resp.StatusCode)
	}

	return nil
}

// Abort cancels the upload. If an upload is started but not completed, the storage space will
// be counted against your AWS account (and getting rid of it is difficult), so you should make
// sure either Abort or Complete is called.
//
// For your convenience, Abort is set as the finalizer for S3Multipart objects as a failsafe, but
// you shouldn't rely on that.
func (mp *S3Multipart) Abort() error {
	mp.lock.Lock()
	defer mp.lock.Unlock()

	if mp.completed {
		return fmt.Errorf("s3: cannot call Abort on an aborted multipart request")
	}

	values := url.Values{}
	values.Set("uploadId", mp.uploadId)

	req, er := http.NewRequest("DELETE", mp.s3.resource(mp.key, values), nil)
	if er != nil {
		return er
	}

	resp, er := http.DefaultClient.Do(req)
	if er != nil {
		return er
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("s3: Abort returned an error (HTTP %d)", resp.StatusCode)
	}

	mp.completed = true
	return nil
}
