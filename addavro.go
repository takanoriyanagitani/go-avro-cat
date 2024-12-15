package addavro

const BlobSizeMaxDefault int = 1048576

type InputConfig struct {
	BlobSizeMax int
}

var InputConfigDefault InputConfig = InputConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type OutputConfig struct {
	Codec
	BlockLength int
}

var OutputConfigDefault OutputConfig = OutputConfig{
	Codec:       CodecNull,
	BlockLength: BlockLengthDefault,
}
