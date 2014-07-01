package s3

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type S3Error struct {
	Code        int
	ShouldRetry bool
	Body        []byte
}

func wrapError(resp *http.Response) error {
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	return &S3Error{
		Code:        resp.StatusCode,
		ShouldRetry: resp.StatusCode == http.StatusInternalServerError,
		Body:        bodyBytes,
	}
}

func (err *S3Error) Error() string {
	return fmt.Sprintf("S3 Error: %s %s", err.Code, string(err.Body))
}
