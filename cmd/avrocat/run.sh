#!/bin/sh

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

#cat sample.d/sample1.jsonl | json2avrows > ./sample.d/sample1.avro
#cat sample.d/sample2.jsonl | json2avrows > ./sample.d/sample2.avro

filenames() {
	ls ./sample.d/sample1.avro
	ls ./sample.d/sample2.avro
}

filenames |
	./avrocat |
	rq \
		--input-avro \
		--output-json |
	jaq --compact-output
