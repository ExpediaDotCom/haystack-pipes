/*
 * Copyright 2017 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.kafkaProducer;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.metrics.MetricObjects;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.util.JsonFormat;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.kafkaProducer.Constants.APPLICATION;

public class ProduceIntoExternalKafkaAction implements ForeachAction<String, Span> {
    static final ExternalKafkaConfigurationProvider EKCP = new ExternalKafkaConfigurationProvider();
    private static final String TOPIC = EKCP.totopic();
    private static final String CLASS_NAME = ProduceIntoExternalKafkaAction.class.getSimpleName();
    private static final MetricObjects METRIC_OBJECTS = new MetricObjects();
    static final String ERROR_MSG = "Problem posting JSON [%s] to Kafka";
    static final String DEBUG_MSG = "Sent JSON [%s] to Kafka topic [%s]";
    static final Counter REQUEST = METRIC_OBJECTS.createAndRegisterCounter(SUBSYSTEM, APPLICATION, CLASS_NAME, "REQUEST");
    static JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
    static Timer KAFKA_PRODUCER_POST = METRIC_OBJECTS.createAndRegisterBasicTimer(SUBSYSTEM, APPLICATION, CLASS_NAME,
            "KAFKA_PRODUCER_POST", TimeUnit.MICROSECONDS);
    static Logger logger = LoggerFactory.getLogger(ProduceIntoExternalKafkaAction.class);
    static Factory factory = new Factory();

    static KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getConfigurationMap());

    public void apply(String key, Span value) {
        REQUEST.increment();
        final Stopwatch stopwatch = KAFKA_PRODUCER_POST.start();
        try {
            final String jsonWithOpenTracingTags = printer.print(value);
            final String jsonWithFlattenedTags = flattenTags(jsonWithOpenTracingTags);
            final ProducerRecord<String, String> producerRecord = factory.createProducerRecord(key, jsonWithFlattenedTags);
            final Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(producerRecord);
            if(EKCP.waitforresponse()) {
                final RecordMetadata recordMetadata = recordMetadataFuture.get();
                if(logger.isDebugEnabled()) {
                    logger.debug(String.format(DEBUG_MSG, value, recordMetadata.partition()));
                }
            }
        } catch (Exception exception) {
            logger.error(ERROR_MSG, value, exception);
        } finally {
            stopwatch.stop();
        }
    }

    private static String flattenTags(String jsonWithOpenTracingTags) {
        final String tagsKey = "tags";
        final JsonObject jsonObject = new Gson().fromJson(jsonWithOpenTracingTags, JsonObject.class);
        final JsonArray jsonArray = jsonObject.getAsJsonArray(tagsKey);
        final JsonElement openTracingTags = jsonObject.remove(tagsKey);
        if(openTracingTags != null) {
            final JsonObject flattenedTagMap = new JsonObject();
            jsonObject.add(tagsKey, flattenedTagMap);
            for (final JsonElement jsonElement : jsonArray) {
                final JsonObject tagMap = jsonElement.getAsJsonObject();
                final String key = tagMap.get("key").getAsString();
                final JsonElement vStr = tagMap.get("vStr");
                if (vStr != null) {
                    flattenedTagMap.addProperty(key, vStr.getAsString());
                    continue;
                }
                final JsonElement vLong = tagMap.get("vLong");
                if (vLong != null) {
                    flattenedTagMap.addProperty(key, vLong.getAsLong());
                    continue;
                }
                final JsonElement vDouble = tagMap.get("vDouble");
                if (vDouble != null) {
                    flattenedTagMap.addProperty(key, vDouble.getAsDouble());
                    continue;
                }
                final JsonElement vBool = tagMap.get("vBool");
                if (vBool != null) {
                    flattenedTagMap.addProperty(key, vBool.getAsBoolean());
                    continue;
                }
                final JsonElement vBytes = tagMap.get("vBytes");
                flattenedTagMap.addProperty(key, vBytes.getAsString());
            }
        }
        return jsonObject.toString();
    }

    private static Map<String, Object> getConfigurationMap() {
        final Map<String, Object> map = new HashMap<>();
        map.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, EKCP.brokers() + ":" + EKCP.port());
        map.put(ProducerConfig.ACKS_CONFIG, EKCP.acks());
        map.put(ProducerConfig.RETRIES_CONFIG, 0);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, EKCP.batchsize());
        map.put(ProducerConfig.LINGER_MS_CONFIG, EKCP.lingerms());
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, EKCP.buffermemory());
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return map;
    }

    static class Factory {
        ProducerRecord<String, String> createProducerRecord(String key, String value) {
            return new ProducerRecord<>(TOPIC, key, value);
        }
    }
}
