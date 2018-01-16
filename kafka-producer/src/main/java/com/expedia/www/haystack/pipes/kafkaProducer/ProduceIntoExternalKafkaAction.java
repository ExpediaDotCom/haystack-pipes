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
import com.netflix.servo.util.VisibleForTesting;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.expedia.www.haystack.pipes.commons.CommonConstants.SUBSYSTEM;
import static com.expedia.www.haystack.pipes.kafkaProducer.Constants.APPLICATION;

public class ProduceIntoExternalKafkaAction implements ForeachAction<String, Span> {
    private static final ExternalKafkaConfigurationProvider EKCP = new ExternalKafkaConfigurationProvider();
    private static final String TOPIC = EKCP.totopic();
    private static final String CLASS_NAME = ProduceIntoExternalKafkaAction.class.getSimpleName();
    private static final MetricObjects METRIC_OBJECTS = new MetricObjects();
    private static final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

    @VisibleForTesting static final String ERROR_MSG =
            "Exception posting JSON [%s] to Kafka; received message [%s]";
    @VisibleForTesting static Counter REQUEST =
            METRIC_OBJECTS.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION, CLASS_NAME, "REQUEST");
    @VisibleForTesting static Counter POSTS_IN_FLIGHT =
            METRIC_OBJECTS.createAndRegisterResettingCounter(SUBSYSTEM, APPLICATION, CLASS_NAME, "POSTS_IN_FLIGHT");
    static ObjectPool<ProduceIntoExternalKafkaCallback> objectPool = new GenericObjectPool<>(new CallbackFactory());

    @VisibleForTesting static Timer KAFKA_PRODUCER_POST = METRIC_OBJECTS.createAndRegisterBasicTimer(
            SUBSYSTEM, APPLICATION, CLASS_NAME, "KAFKA_PRODUCER_POST", TimeUnit.MICROSECONDS);
    @VisibleForTesting static Logger logger = LoggerFactory.getLogger(ProduceIntoExternalKafkaAction.class);
    @VisibleForTesting static Factory factory = new Factory();

    static KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getConfigurationMap());

    public void apply(String key, Span value) {
        REQUEST.increment();
        final Stopwatch stopwatch = KAFKA_PRODUCER_POST.start();
        String jsonWithFlattenedTags = "";
        try {
            final String jsonWithOpenTracingTags = printer.print(value);
            jsonWithFlattenedTags = flattenTags(jsonWithOpenTracingTags);
            final ProducerRecord<String, String> producerRecord =
                    factory.createProducerRecord(key, jsonWithFlattenedTags);

            final ProduceIntoExternalKafkaCallback callback = objectPool.borrowObject(); // callback must returnObject()
            POSTS_IN_FLIGHT.increment();
            // TODO Put the Span value into the callback so that it can write it to Kafka for retry

            kafkaProducer.send(producerRecord, callback);
        } catch (Exception exception) {
            // Must format below because log4j2 underneath slf4j doesn't handle .error(varargs) properly
            final String message = String.format(ERROR_MSG, jsonWithFlattenedTags, exception.getMessage());
            logger.error(message, exception);
        } finally {
            stopwatch.stop();
        }
    }

    static String flattenTags(String jsonWithOpenTracingTags) {
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
                if(vBytes != null) {
                    flattenedTagMap.addProperty(key, vBytes.getAsString());
                }
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

    static class CallbackFactory extends BasePooledObjectFactory<ProduceIntoExternalKafkaCallback> {
        @Override
        public ProduceIntoExternalKafkaCallback create() {
            return new ProduceIntoExternalKafkaCallback();
        }

        @Override
        public PooledObject<ProduceIntoExternalKafkaCallback> wrap(
                ProduceIntoExternalKafkaCallback produceIntoExternalKafkaCallback) {
            return new DefaultPooledObject<>(produceIntoExternalKafkaCallback);
        }
    }
}
