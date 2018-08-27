export DOCKER_ORG := expediadotcom
export DOCKER_IMAGE_NAME := haystack-pipes-firehose-writer

docker_build:
	docker build -t $(DOCKER_IMAGE_NAME) -f build/docker/Dockerfile .

release:
	../scripts/publish-to-docker-hub.sh
