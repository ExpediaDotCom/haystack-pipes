##Sample SpanKey Extractor

It is a sample project that use java SPI to use [Sample Extractor](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/sample-key-extractor/src/main/java/com/expedia/www/haystack/pipes/kafkaProducer/extractor/SampleExtractor.java) as extractor that transforms span to json.
[Dockerfile](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/sample-key-extractor/build/docker/Dockerfile) helps to deploy this project. The configuration required by the extractor is already mentioned in [base.conf](https://github.com/ExpediaDotCom/haystack-pipes/blob/master/kafka-producer/src/main/resources/config/base.conf). 

