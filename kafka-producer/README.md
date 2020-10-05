# kafka-producer

The `kafka-producer` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read protobuf
records from Kafka, it uses SPI model to load key-extractor to transform span, and write the transformed record to another (typically
external) Kafka queue and topic. The code is simple and self-explanatory and consists of the following classes:
1. Span Key extractor is required for `kafka-producer` to work. The [sample extractor](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/sample-key-extractor/src/main/java/com/expedia/www/haystack/pipes/kafkaProducer/extractor/SampleExtractor.java)
transforms span to tags-flattened JSON.
2. A [Kafka producer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafka/producer/ProtobufToKafkaProducer.java)
that wires serializers and deserializers into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
3. A simple java [application](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafka/producer/Service.java)
that provides an HTTP endpoint, used for health checks.
4. [Configurations](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafka/producer/config/KafkaProducerConfig.java)
for the external (probably)/another Kafka queue to which the Spans' JSON is written.
5. A [Kafka for/each action](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/java/com/expedia/www/haystack/pipes/kafka/producer/KafkaToKafkaPipeline.java)
at the end the Kafka Streams pipeline that writes ("produces") to the external Kafka queue.
6. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/kafka-producer/src/test/java/com/expedia/www/haystack/pipes/kafka/producer)

Various classes from the [commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons)
package are also used.

###Deployment
 Docker image of this module can be made using [Dockerfile](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/build/docker/Dockerfile).
 The above docker image is not the deploy-ready image. It would require Extractors provided by Java SPI model.
 
 For eg: building docker image using [Dockerfile](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/sample-key-extractor/build/docker/Dockerfile), 
 would give you [Sample Extractor](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/sample-key-extractor/src/main/java/com/expedia/www/haystack/pipes/kafkaProducer/extractor/SampleExtractor.java).
 with this module.The configurations for Sample Extractors are added in [base.conf](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/resources/config/base.conf).
                                                                          
 
 You can load more than one extractors by adding code in the project and the file paths in [file](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/sample-key-extractor/src/main/resources/META-INF/services)
 and configurations required can be added in [base.conf](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/resources/config/base.conf). 
 Or we can override the conf file using environment variable as `HAYSTACK_OVERRIDES_CONFIG_PATH`.
 