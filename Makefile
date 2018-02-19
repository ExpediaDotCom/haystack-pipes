PWD := $(shell pwd)

all: clean build

clean:
	mvn clean

build: clean
	mvn package

json-transformer:
	mvn package -pl json-transformer -am

kafka-producer:
	mvn package -pl kafka-producer -am

http-poster:
	mvn package -pl http-poster -am

firehose-writer:
	mvn package -pl firehose-writer -am

# build all and release
release: all
	cd json-transformer && $(MAKE) release
	cd kafka-producer && $(MAKE) release
	cd http-poster && $(MAKE) release
	cd firehose-writer && $(MAKE) release
