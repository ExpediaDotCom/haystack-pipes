PWD := $(shell pwd)

all: clean build

clean:
	mvn clean

build: clean
	mvn package

json-transformer:
	mvn package -pl json-transformer -am

# build all and release
release: all
	cd json-transformer && $(MAKE) release
