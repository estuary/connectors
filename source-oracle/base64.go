package main

import (
	"strings"
)

// Base64 order
var base64Order = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
var base64Index = func() map[rune]int {
	m := make(map[rune]int)
	for i, ch := range base64Order {
		m[ch] = i
	}
	return m
}()

// Helper function to generate a sort key for a Base64 string
func base64SortKey(s string) []int {
	s = strings.TrimRight(s, "=") // Remove padding
	key := make([]int, len(s))
	for i, ch := range s {
		key[i] = base64Index[ch]
	}
	return key
}

func base64LessThan(a, b string) bool {
	keyA := base64SortKey(a)
	keyB := base64SortKey(b)
	for i := 0; i < len(keyA) && i < len(keyB); i++ {
		if keyA[i] != keyB[i] {
			return keyA[i] < keyB[i]
		}
	}

	return len(keyA) < len(keyB)
}
