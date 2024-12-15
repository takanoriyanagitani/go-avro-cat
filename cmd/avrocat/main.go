package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	aa "github.com/takanoriyanagitani/go-avro-cat"
	util "github.com/takanoriyanagitani/go-avro-cat/util"

	dh "github.com/takanoriyanagitani/go-avro-cat/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-cat/avro/enc/hamba"
)

var EnvVarByKey func(string) util.IO[string] = util.Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var schemaFilename util.IO[string] = EnvVarByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) util.IO[string] {
	return util.Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}
		defer f.Close()
		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}
		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeLimit int64 = 1048576

var schemaContent util.IO[string] = util.Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeLimit),
)

var stdin2names2maps util.IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToFilenamesToMapsDefault

var outCfg aa.OutputConfig = aa.OutputConfigDefault

var encCfg util.IO[eh.EncodeConfig] = util.Bind(
	schemaContent,
	util.Lift(func(s string) (eh.EncodeConfig, error) {
		return eh.EncodeConfig{
			Schema:       s,
			OutputConfig: outCfg,
		}, nil
	}),
)

var stdin2names2avros2stdout util.IO[util.Void] = util.Bind(
	encCfg,
	func(cfg eh.EncodeConfig) util.IO[util.Void] {
		return util.Bind(
			stdin2names2maps,
			eh.ConfigToMapsToStdout(cfg),
		)
	},
)

var sub util.IO[util.Void] = func(ctx context.Context) (util.Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2names2avros2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
