package ingestion

import (
	"time"

	"github.com/pransuu69/meritual-harness-webhook/internal/models"
	"github.com/valyala/fastjson"
)

// Parser extracts commit information from raw webhook JSON payloads
// using a pooled zero-allocation fast parser.
type Parser struct {
	pool fastjson.ParserPool
}

// NewParser creates a Parser.
func NewParser() *Parser {
	return &Parser{}
}

// ParseCommits extracts all commits from a GitHub push event payload.
// It handles both standard GitHub push webhook payloads (with `before`/`after` or `commits` array)
// and normalized payloads containing `commit_sha` and `parent_sha`.
func (p *Parser) ParseCommits(body []byte) ([]models.Commit, error) {
	parser := p.pool.Get()
	defer p.pool.Put(parser)

	v, err := parser.ParseBytes(body)
	if err != nil {
		return nil, err
	}

	var results []models.Commit

	// Case 1: Direct normalized schema: `commit_sha` and `parent_sha`
	if v.Exists("commit_sha") {
		c := models.Commit{
			SHA:       string(v.GetStringBytes("commit_sha")),
			ParentSHA: string(v.GetStringBytes("parent_sha")),
			Author:    string(v.GetStringBytes("author")),
			Message:   string(v.GetStringBytes("message")),
		}
		if tsBytes := v.GetStringBytes("timestamp"); len(tsBytes) > 0 {
			if parsedTime, err := time.Parse(time.RFC3339, string(tsBytes)); err == nil {
				c.Timestamp = parsedTime
			}
		}
		if c.Timestamp.IsZero() {
			c.Timestamp = time.Now()
		}
		results = append(results, c)
		return results, nil
	}

	// Case 2: Standard GitHub Push Event schema
	beforeSHA := string(v.GetStringBytes("before"))
	afterSHA := string(v.GetStringBytes("after"))

	commitsArray := v.GetArray("commits")
	if len(commitsArray) > 0 {
		prevParent := beforeSHA
		for _, commitObj := range commitsArray {
			sha := string(commitObj.GetStringBytes("id"))
			if sha == "" {
				sha = string(commitObj.GetStringBytes("sha"))
			}

			// If specific parent_sha is present in commit object
			parent := string(commitObj.GetStringBytes("parent_sha"))
			if parent == "" {
				parent = prevParent
			}

			c := models.Commit{
				SHA:       sha,
				ParentSHA: parent,
				Message:   string(commitObj.GetStringBytes("message")),
			}

			if authorObj := commitObj.Get("author"); authorObj != nil {
				c.Author = string(authorObj.GetStringBytes("name"))
			}

			if tsBytes := commitObj.GetStringBytes("timestamp"); len(tsBytes) > 0 {
				if parsedTime, err := time.Parse(time.RFC3339, string(tsBytes)); err == nil {
					c.Timestamp = parsedTime
				}
			}
			if c.Timestamp.IsZero() {
				c.Timestamp = time.Now()
			}

			results = append(results, c)
			prevParent = sha
		}
	} else if afterSHA != "" {
		// Single push event without explicit commits array (or deleted branch / tag push)
		c := models.Commit{
			SHA:       afterSHA,
			ParentSHA: beforeSHA,
			Timestamp: time.Now(),
		}
		results = append(results, c)
	}

	return results, nil
}
