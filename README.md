[![Coverage Status](https://coveralls.io/repos/github/ExpediaDotCom/haystack-pipes/badge.svg?branch=master)](https://coveralls.io/github/ExpediaDotCom/haystack-pipes?branch=master)
[Build Status](https://travis-ci.org/ExpediaDotCom/haystack-pipes)

# haystack-pipes
Packages to send ("pipe") Haystack data to external sinks (like AWS Firehose)
![High Level Block Diagram](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/documents/diagrams/haystack_pipes.png)

The haystack-pipes unit delivers a human-friendly version of Haystack messages to zero or more "durable" locations for 
more permanent storage. Current "plug`in" candidates for such storage include:
1. [Kafka Producer](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/kafka-producer)
2. [Amazon Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/) is an AWS service that facilitates loading 
streaming data into AWS. Note that its 
[PutRecordBatch API](http://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html) accepts up to
500 records, with a maximum size of 4 MB for each put request. The plug in will batch the records appropriately.
Kinesis Firehose can be configured to deliver the data to
    * [Amazon S3](https://aws.amazon.com/s3/)
    * [Amazon Redshift](https://aws.amazon.com/redshift/)
    * [Amazon Elasticsearch Service](https://aws.amazon.com/elasticsearch-service/)
    
#### json-transformer    
The [json-transformer](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/json-transformer) package is a
lightweight service that uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read the protobuf 
records from Kafka, transform them to JSON, and write them to another topic in Kafka. The plugins will then consume
from the latter topic, and then write the JSON records just consumed to their destinations.

#### kafka-producer

#### firehose-writer

## Building

#### Cloning
##### From scratch
Since this repo contains haystack-idl as a submodule, a recursive clone of the
[haystack-pipes package](https://github.com/ExpediaDotCom/haystack-pipes) is required:

```git clone --recursive git@github.com:ExpediaDotCom/haystack-pipes.git .```

##### From existing directory
If you have already cloned the the [haystack-pipes package](https://github.com/ExpediaDotCom/haystack-pipes) (perhaps
with an IDE that did not clone recursively as the command above instructs), or if you want to pick up a newer version of
the [haystack-idl package](https://github.com/ExpediaDotCom/haystack-idl), run the following from your haystack-pipes
directory:

```git submodule update --init --recursive```

#### Prerequisites: 

* Java 1.8
* Maven 3.3.9 or higher
* Docker 1.13 or higher

#### Build

##### Full build
For a full build, including unit tests and Docker image build, run:

```
make all
```
##### Individual package build
In place of <package name> below, put the name of the package you want to build (e.g. json-transformer)

```
cd <package name>
mvn clean package # to run the unit tests and build the jar file
make docker-image # to build the Docker image
```
When the build runs on Travis, it will upload the Docker image to Docker Hub.