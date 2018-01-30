package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.serialization.SpanSerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public abstract class KafkaStreamBuilderBase implements KafkaStreamBuilder {
    private final KafkaStreamStarter kafkaStreamStarter;
    private final SpanSerdeFactory spanSerdeFactory;
    private final String application;
    private final KafkaConfigurationProvider kafkaConfigurationProvider;
    private final ForeachAction<String, Span> foreachAction;

    public KafkaStreamBuilderBase(KafkaStreamStarter kafkaStreamStarter,
                                  SpanSerdeFactory spanSerdeFactory,
                                  String application,
                                  KafkaConfigurationProvider kafkaConfigurationProvider,
                                  ForeachAction<String, Span> foreachAction) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.spanSerdeFactory = spanSerdeFactory;
        this.application = application;
        this.kafkaConfigurationProvider = kafkaConfigurationProvider;
        this.foreachAction = foreachAction;
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Span> spanSerde = spanSerdeFactory.createSpanSerde(application);
        final String fromTopic = kafkaConfigurationProvider.fromtopic();
        final KStream<String, Span> stream = kStreamBuilder.stream(stringSerde, spanSerde, fromTopic);
        stream.foreach(foreachAction);
    }

    /**
     * main() is an instance method because it is called by the static void *IsActiveController.main(String [] args)
     * methods; making it an instance method facilitates unit testing.
     */
    public void main() {
        kafkaStreamStarter.createAndStartStream(this);
    }

}
