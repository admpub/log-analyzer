package similarity

import (
	"fmt"
	"testing"
)

var demoLines = [][]string{
	{
		"Hello",
		"World",
		"hello",
		"world",
		"Hello World",
		"test",
		"testing",
		"test1",
		"Test",
	},
	{
		"a",
		"b",
		"aa",
		"bbb",
		"aba",
		"aaa",
		"c",
		"cc",
		"abc",
	},
	{
		"test",
		"test",
		"test",
		"test",
		"test",
		"testing",
		"Testing",
		"Test",
		"Test",
		"Test",
		"Test",
		"Test",
	},
	{
		"cat dog mouse elephant deer bear",
		"cat dog mouse elephant",
		"deer bear",
		"car bear",
		"car bear",
		"car bear elephant mouse",
		"car bear elephant deer",
		"dog",
		"dog cat",
		"beat deet elephant moust dot cat",
		"bear deer elephant mouse dog cat",
		"bear deer elephant mouse dog",
		"elephant",
		"elehant",
		"god tac",
		"god",
		"god dog",
		"goddog",
		"goddogcat",
		"goddogcatelepahnt",
		"bear deer",
		"bedeerar",
		"animal",
	},
}

func TestFindGroups(t *testing.T) {
	for _, lines := range demoLines {
		groups := FindGroups(lines)
		fmt.Println(groups)
	}
}
