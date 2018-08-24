PWD := $(shell pwd)

all: clean build

clean:
	mvn clean

build: clean
	mvn package

json-transformer:
	mvn package -DfinalName=haystack-pipes-json-transformer -pl json-transformer -am

kafka-producer:
	mvn package -DfinalName=haystack-pipes-kafka-produce -pl kafka-producer -am

http-poster:
	mvn package -DfinalName=haystack-pipes-http-poster -pl http-poster -am

firehose-writer:
	mvn package -DfinalName=haystack-pipes-firehose-writer -pl firehose-writer -am

secret-detector:
	mvn package -DfinalName=haystack-pipes-secret-detector -pl secret-detector -am

# build all and release
release: all
	cd json-transformer && $(MAKE) release
	cd kafka-producer && $(MAKE) release
	cd http-poster && $(MAKE) release
	cd firehose-writer && $(MAKE) release
	cd secret-detector && $(MAKE) release
	./.travis/deploy.sh
