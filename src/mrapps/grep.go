package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"finalproj/mr"
)

// Compile-time keyword variable.
var keyword string

func init() {
	// Use a default keyword if none is provided at build time.
	keyword = os.Getenv("GREP_KEYWORD")
	if keyword == "" {
		// Default keyword.
		keyword = "world"
	}
	fmt.Printf("Keyword: %s\n", keyword)
}

// The map function processes the file and tracks file name and line number.
func Map(filename string, contents string) []mr.KeyValue {
	// Split contents into lines.
	lines := strings.Split(contents, "\n")

	kva := []mr.KeyValue{}
	for i, line := range lines {
		if strings.Contains(line, keyword) {
			// Use a combination of file name and line number as the key.
			// The value is the matched line content.
			key := "(" + filename + ":" + strconv.Itoa(i+1) + ")" // Line numbers are 1-based.
			kv := mr.KeyValue{key, line}
			kva = append(kva, kv)
		}
	}
	return kva
}

// The reduce function collects all occurrences of each file-line combination.
func Reduce(key string, values []string) string {
	// Each key corresponds to a unique file and line number.
	// Return the line content (it's the same for all values).
	return strings.Join(values, "\n")
}
