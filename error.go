package s3

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
)

type S3Error struct {
	Code        int
	ShouldRetry bool
	Body        []byte
}

type S3NewEndpointError struct {
	Code     string
	Message  string
	Bucket   string
	Endpoint string
}

func wrapError(resp *http.Response) *S3Error {
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	return &S3Error{
		Code:        resp.StatusCode,
		ShouldRetry: resp.StatusCode == http.StatusInternalServerError,
		Body:        bodyBytes,
	}
}

func (err *S3Error) Error() string {
	return fmt.Sprintf("S3 Error: %d %s", err.Code, string(err.Body))
}

func (err *S3Error) newEndpoint() string {
	msg := S3NewEndpointError{}

	if er := xml.Unmarshal(err.Body, &msg); er == nil {
		if msg.Code == "TemporaryRedirect" {
			return msg.Endpoint
		}
	}

	return ""
}
