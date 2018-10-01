# secret-detector

The `secret-detector` service uses [Kafka Streams](https://kafka.apache.org/documentation/streams/) to read protobuf
records from Kafka, check them for sensitive data, and send a warning email if sensitive data is found.
The code is simple and self-explanatory and consists of the following classes:
1. A [Detector](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/secret-detector/src/main/java/com/expedia/www/haystack/pipes/secretDetector/Detector.java)
that uses the open source [chlorine-finder](https://github.com/dataApps/chlorine-finder) 
package to detect sensitive data.
2. A [Detector Action](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/secret-detector/src/main/java/com/expedia/www/haystack/pipes/secretDetector/DetectorAction.java)
that sends alerts (currently just emails) if sensitive data is found.
3. A [Detector Producer](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/secret-detector/src/main/java/com/expedia/www/haystack/pipes/secretDetector/DetectorAction.java)
that wires the Detector Action into a
[Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams) pipeline.
4. [Configurations](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/secret-detector/src/main/java/com/expedia/www/haystack/pipes/secretDetector/SecretsConfigurationProvider.java)
for the email address(es) to which the alerts should be sent.
5. A simple Spring Boot [application](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/secret-detector/src/main/java/com/expedia/www/haystack/pipes/secretDetector/DetectorIsActiveController.java)
that provides an HTTP endpoint, used for health checks.
6. [Unit tests](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/secret-detector/src/test/java/com/expedia/www/haystack/pipes/secretDetector)

Various classes from the [pipes-commons](https://github.com/ExpediaDotCom/haystack-pipes/tree/master/commons) package 
are also used. Finders from the open source [chlorine-finder](https://github.com/dataApps/chlorine-finder) package
detect secrets, along with "custom finders" provided by Haystack. Some of this code has been migrated to the
[haystack-commons](https://github.com/ExpediaDotCom/haystack-commons) package so that it can be used by other services.
