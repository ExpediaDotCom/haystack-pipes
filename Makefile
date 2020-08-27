.PHONY: all clean release json-transformer kafka-producer http-poster firehose-writer secret-detector span-key-extractor

PWD := $(shell pwd)

all: clean build

clean:
	mvn clean

build: clean
	mvn package

json-transformer:
	mvn package -DfinalName=haystack-pipes-json-transformer -pl json-transformer -am

kafka-producer:
	mvn package -DfinalName=haystack-pipes-kafka-producer -pl kafka-producer -am

http-poster:
	mvn package -DfinalName=haystack-pipes-http-poster -pl http-poster -am

firehose-writer:
	mvn package -DfinalName=haystack-pipes-firehose-writer -pl firehose-writer -am

secret-detector:
	mvn package -DfinalName=haystack-pipes-secret-detector -pl secret-detector -am

span-key-extractor:
	mvn package -DfinalName=span-key-extractor -pl span-key-extractor -am

sample-key-extractor:
	mvn package -DfinalName=sample-key-extractor -pl sample-key-extractor -am

# build all and release
release: clean span-key-extractor json-transformer kafka-producer http-poster firehose-writer secret-detector
	cd json-transformer && $(MAKE) release
	cd kafka-producer && $(MAKE) release
	cd http-poster && $(MAKE) release
	cd firehose-writer && $(MAKE) release
	cd secret-detector && $(MAKE) release
	./.travis/deploy.sh
