package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

const sample = `{
    "type": "tx",
    "ops": [
        {
            "op": "set",
            "ks": "users",
            "k": "1234",
            "v": "asmith"
        },
        {
            "op": "set",
            "ks": "users",
            "k": "1234/first_name",
            "v": "Anna"
        },
        {
            "op": "set",
            "ks": "user_groups",
            "k": "asmith",
            "v": "admin"
        }
    ]
}`

var input = []byte(sample)

type Tx struct {
	Type string
	Ops  []struct {
		Op string
		Ks string
		K  string
		V  string
	}
}

func BenchmarkParsingToStruct(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ob := Tx{}
		json.Unmarshal(input, &ob)
	}
}

func BenchmarkParsingToStructVar(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ob Tx
		json.Unmarshal(input, &ob)
	}
}

func BenchmarkParsingToMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var ob interface{}
		json.Unmarshal(input, &ob)
	}
}

func BenchmarkDecodingToStruct(b *testing.B) {
	reader := bytes.NewReader(input)

	for i := 0; i < b.N; i++ {
		command := &Tx{}
		json.NewDecoder(reader).Decode(&command)
		reader.Seek(0, 0)
	}
}
