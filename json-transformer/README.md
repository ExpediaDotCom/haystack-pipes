# json-transformer

The `json-transformer` service that uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read the 
protobuf records from Kafka, transform them to JSON, and write the transformed record to another topic in Kafka.
The code is simple and self-explanatory and consists of the following classes:
1. A [deserializer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/json-transformer/src/main/java/com/expedia/www/haystack/pipes/SpanProtobufDeserializer.java)
that reads the [protobuf](https://en.wikipedia.org/wiki/Protocol_Buffers)
[Span](https://github.com/ExpediaDotCom/haystack-idl/blob/master/proto/span.proto) records from Kafka.
2. A [serializer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/json-transformer/src/main/java/com/expedia/www/haystack/pipes/SpanJsonSerializer.java)
that writes the Span records into another Kafka topic, writing them in [JSON](https://en.wikipedia.org/wiki/JSON).
3. A [transformer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/json-transformer/src/main/java/com/expedia/www/haystack/pipes/ProtobufToJsonTransformer.java)
that wires the deserializer and serializer into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
4. An [uncaught exception handler](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/json-transformer/src/main/java/com/expedia/www/haystack/pipes/SystemExitUncaughtExceptionHandler.java)
that brings down the [JVM](https://en.wikipedia.org/wiki/Java_virtual_machine) if an exception occurs in the
KafkaStreams code but cannot be handled by that code. Typically this involves an unusual issue (disk full, insufficient
memory, [Zookeeper](https://en.wikipedia.org/wiki/Apache_ZooKeeper) problems), and it is expected that the
[Kubernetes](https://en.wikipedia.org/wiki/Kubernetes) 
[infrastructure](https://github.com/ExpediaDotCom/haystack/tree/master/deployment/k8s) in
[Haystack](https://github.com/ExpediaDotCom/haystack) will restart the json-transformer process.
5. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/json-transformer/src/test/java/com/expedia/www/haystack/pipes)