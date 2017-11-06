# json-transformer

The `json-transformer` service provides shared classes used byr other modules in the 
[haystack-pipes](https://github.com/ExpediaDotCom/haystack-pipes) package. These classes fall into several categories: 
1. [Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams): see the classes
[here](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/commons/src/main/java/com/expedia/www/haystack/pipes/commons/kafka)
2. Serializers and deserializers: see the classes
[here](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/commons/src/main/java/com/expedia/www/haystack/pipes/commons/serialization)
3. An [uncaught exception handler](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/commons/src/main/java/com/expedia/www/haystack/pipes/commons/SystemExitUncaughtExceptionHandler.java)
that brings down the [JVM](https://en.wikipedia.org/wiki/Java_virtual_machine) if an exception occurs in the
KafkaStreams code but cannot be handled by that code. Typically this involves an unusual issue (disk full, insufficient
memory, [Zookeeper](https://en.wikipedia.org/wiki/Apache_ZooKeeper) problems), and it is expected that the
[Kubernetes](https://en.wikipedia.org/wiki/Kubernetes) 
[infrastructure](https://github.com/ExpediaDotCom/haystack/tree/master/deployment/k8s) in
[Haystack](https://github.com/ExpediaDotCom/haystack) will restart the json-transformer process.
4. A [configuration source](https://static.javadoc.io/org.cfg4j/cfg4j-core/4.4.0/org/cfg4j/source/ConfigurationSource.html)
implementation that changes the keys of environment variables to lower case, per conventation: see
[here](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/commons/src/main/java/com/expedioa/www/haystack/pipes/commons/ChangeEnvVarsToLowerCaseConfigurationSource.java)
5. Other miscellaneous classes; see
[here](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/commons/src/main/java/com/expedioa/www/haystack/pipes/commons)
for details.