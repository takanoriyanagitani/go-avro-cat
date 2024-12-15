package dec

import (
	"bufio"
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	aa "github.com/takanoriyanagitani/go-avro-cat"
	util "github.com/takanoriyanagitani/go-avro-cat/util"
)

func ConfigToFuncs(cfg aa.InputConfig) []ho.DecoderFunc {
	var blobSizeMax int = cfg.BlobSizeMax
	var hcfg ha.Config
	hcfg.MaxByteSliceSize = blobSizeMax
	var hapi ha.API = hcfg.Freeze()
	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMapsHamba(
	rdr io.ReadCloser,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		defer rdr.Close()
		var br io.Reader = bufio.NewReader(rdr)

		dec, e := ho.NewDecoder(br, opts...)
		if nil != e {
			yield(nil, e)
			return
		}

		var m map[string]any
		for dec.HasNext() {
			clear(m)
			e := dec.Decode(&m)
			if !yield(m, e) {
				return
			}
		}
	}
}

func ReaderToMaps(
	rdr io.ReadCloser,
	cfg aa.InputConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMapsHamba(rdr, ConfigToFuncs(cfg)...)
}

func FilenamesToMaps(
	names iter.Seq[string],
	cfg aa.InputConfig,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		var opts []ho.DecoderFunc = ConfigToFuncs(cfg)
		for name := range names {
			file, e := os.Open(name)
			if nil != e {
				if !yield(nil, e) {
					return
				}
				continue
			}

			var pairs iter.Seq2[map[string]any, error] = ReaderToMapsHamba(
				file,
				opts...,
			)
			for m, e := range pairs {
				if !yield(m, e) {
					return
				}
			}
		}
	}
}

func ReaderToFilenames(
	rdr io.Reader,
) iter.Seq[string] {
	return func(yield func(string) bool) {
		var s *bufio.Scanner = bufio.NewScanner(rdr)
		for s.Scan() {
			var name string = s.Text()
			if !yield(name) {
				return
			}
		}
	}
}

func StdinToFilenamesToMaps(
	cfg aa.InputConfig,
) iter.Seq2[map[string]any, error] {
	var names iter.Seq[string] = ReaderToFilenames(os.Stdin)
	return FilenamesToMaps(names, cfg)
}

func ConfigToStdinToFilenamesToMaps(
	cfg aa.InputConfig,
) util.IO[iter.Seq2[map[string]any, error]] {
	return func(_ context.Context) (iter.Seq2[map[string]any, error], error) {
		return StdinToFilenamesToMaps(cfg), nil
	}
}

var StdinToFilenamesToMapsDefault util.
	IO[iter.Seq2[map[string]any, error]] = ConfigToStdinToFilenamesToMaps(
	aa.InputConfigDefault,
)
