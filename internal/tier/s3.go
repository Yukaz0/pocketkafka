// Package tier implements tiered (cold) storage for go-kafka-neu: old segments
// are offloaded to an S3/MinIO-compatible object store and fetched back on
// demand. It uses a zero-dependency HTTP client with AWS Signature V4 signing.
package tier

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// S3Client is a minimal S3-compatible object store client with SigV4 signing.
type S3Client struct {
	Endpoint  string // e.g. http://localhost:9000
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string
	Prefix    string
	HTTP      *http.Client
}

// NewS3Client builds a client for the given endpoint/bucket.
func NewS3Client(endpoint, bucket, accessKey, secretKey, region, prefix string) *S3Client {
	return &S3Client{
		Endpoint:  strings.TrimRight(endpoint, "/"),
		Bucket:    bucket,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    region,
		Prefix:    strings.Trim(prefix, "/"),
		HTTP:      &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *S3Client) objectURL(key string) string {
	k := c.Prefix
	if k != "" {
		k += "/"
	}
	k += key
	return fmt.Sprintf("%s/%s/%s", c.Endpoint, c.Bucket, url.PathEscape(k))
}

// PutObject uploads data to an object key.
func (c *S3Client) PutObject(key string, data []byte) error {
	u := c.objectURL(key)
	body := bytes.NewReader(data)
	payloadHash := sha256hex(data)
	now := time.Now().UTC()
	req, err := http.NewRequest(http.MethodPut, u, body)
	if err != nil {
		return err
	}
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	c.sign(req, now, payloadHash, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("s3 put %s: status %d", key, resp.StatusCode)
	}
	return nil
}

// GetObject downloads an object by key.
func (c *S3Client) GetObject(key string) ([]byte, error) {
	u := c.objectURL(key)
	now := time.Now().UTC()
	emptyHash := sha256hex(nil)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("x-amz-content-sha256", emptyHash)
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	c.sign(req, now, emptyHash, nil)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("s3 get %s: not found", key)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("s3 get %s: status %d", key, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// sign adds the SigV4 Authorization header to a request.
func (c *S3Client) sign(req *http.Request, now time.Time, payloadHash string, query map[string]string) {
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}

	host := req.URL.Host
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := ""
	if query != nil {
		parts := make([]string, 0, len(query))
		for k, v := range query {
			parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
		canonicalQuery = strings.Join(parts, "&")
	}

	canonicalRequest := req.Method + "\n" + canonicalURI + "\n" + canonicalQuery + "\n" +
		canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash
	scope := dateStamp + "/" + region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256hex([]byte(canonicalRequest))

	kDate := hmacSHA256([]byte("AWS4"+c.SecretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	auth := "AWS4-HMAC-SHA256 Credential=" + c.AccessKey + "/" + scope +
		", SignedHeaders=" + signedHeaders + ", Signature=" + signature
	req.Header.Set("Authorization", auth)
}

func sha256hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}
