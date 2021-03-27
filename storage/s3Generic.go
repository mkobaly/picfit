package storage

import (
	"bytes"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ulule/gostorages"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

type S3GenericFile struct {
	io.ReadCloser
	size int64
}

func (f *S3GenericFile) Size() int64 {
	return f.size
}

func (f *S3GenericFile) ReadAll() ([]byte, error) {
	return ioutil.ReadAll(f)
}

type S3Generic struct {
	client   *s3.S3
	bucket   string
	baseURL  string
	location string
}

func NewS3Generic(endpoint string, accessKeyId string, secretAccessKey string, bucket string, baseURL string, location string) *S3Generic {
	s, _ := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Credentials:      credentials.NewStaticCredentials(accessKeyId, secretAccessKey, ""),
		Endpoint:         &endpoint,
		S3ForcePathStyle: aws.Bool(true),
	})
	return &S3Generic{
		client:   s3.New(s),
		bucket:   bucket,
		baseURL:  baseURL,
		location: location,
	}

}

func (s *S3Generic) Save(filepath string, file gostorages.File) error {
	buffer, err := file.ReadAll()
	if err != nil {
		return err
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath),
		Body:   bytes.NewReader(buffer),
	}
	_, err = s.client.PutObject(input)
	return err
}

func (s *S3Generic) Path(filepath string) string {
	return path.Join(s.location, filepath)
}

func (s *S3Generic) Exists(filepath string) bool {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath),
	}
	_, err := s.client.GetObject(input)
	if err != nil {
		return false
	}
	return true

}

func (s *S3Generic) Delete(filepath string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath),
	}
	_, err := s.client.DeleteObject(input)
	return err
}

func (s *S3Generic) Open(filepath string) (gostorages.File, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath),
	}
	resp, err := s.client.GetObject(input)
	if err != nil {
		return nil, err
	}

	return &S3GenericFile{
		ReadCloser: resp.Body,
	}, nil
}

func (s *S3Generic) ModifiedTime(filepath string) (time.Time, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath),
	}
	resp, err := s.client.GetObject(input)
	if err != nil {
		return time.Time{}, err
	}
	return *resp.LastModified, nil

}

func (s *S3Generic) Size(filepath string) int64 {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(filepath),
	}
	resp, err := s.client.GetObject(input)
	if err != nil {
		return 0
	}
	return *resp.ContentLength
}

func (s *S3Generic) URL(filename string) string {
	if s.HasBaseURL() {
		return strings.Join([]string{s.baseURL, s.Path(filename)}, "/")
	}

	return ""
}

func (s *S3Generic) HasBaseURL() bool {
	return s.baseURL != ""
}

func (s *S3Generic) IsNotExist(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchKey:
			return true
		default:
			return false
		}
	}
	return false
}
