.PHONY: all json-transformer release

PWD := $(shell pwd)

clean:
	mvn clean

build: clean
	mvn package

all: clean json-transformer

json-transformer:
	mvn package -pl haystack-pipes-json-transformer -am

# build all and release
release: all
	cd json-transformer && $(MAKE) release
