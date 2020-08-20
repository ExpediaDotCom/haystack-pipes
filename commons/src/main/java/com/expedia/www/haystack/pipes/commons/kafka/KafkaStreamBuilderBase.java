package com.expedia.www.haystack.pipes.commons.kafka;

import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.pipes.commons.kafka.config.KafkaConsumerConfig;
import com.expedia.www.haystack.pipes.commons.serialization.SerdeFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public abstract class KafkaStreamBuilderBase implements KafkaStreamBuilder, Main {
    private final KafkaStreamStarter kafkaStreamStarter;
    private final SerdeFactory serdeFactory;
    private final String application;
    private final KafkaConsumerConfig kafkaConfigurationProvider;
    private final ForeachAction<String, Span> foreachAction;
    private final ProcessorSupplier<String, Span> processorSupplier;

    public KafkaStreamBuilderBase(KafkaStreamStarter kafkaStreamStarter,
                                  SerdeFactory serdeFactory,
                                  String application,
                                  KafkaConsumerConfig kafkaConfigurationProvider,
                                  ForeachAction<String, Span> foreachAction) {
        this(kafkaStreamStarter, serdeFactory, application, kafkaConfigurationProvider, foreachAction, null);

    }

    public KafkaStreamBuilderBase(KafkaStreamStarter kafkaStreamStarter,
                                  SerdeFactory serdeFactory,
                                  String application,
                                  KafkaConsumerConfig kafkaConfigurationProvider,
                                  ProcessorSupplier<String, Span> processorSupplier) {
        this(kafkaStreamStarter, serdeFactory, application, kafkaConfigurationProvider, null, processorSupplier);
    }

    private KafkaStreamBuilderBase(KafkaStreamStarter kafkaStreamStarter,
                                   SerdeFactory serdeFactory,
                                   String application,
                                   KafkaConsumerConfig kafkaConfigurationProvider,
                                   ForeachAction<String, Span> foreachAction,
                                   ProcessorSupplier<String, Span> processorSupplier) {
        this.kafkaStreamStarter = kafkaStreamStarter;
        this.serdeFactory = serdeFactory;
        this.application = application;
        this.kafkaConfigurationProvider = kafkaConfigurationProvider;
        this.foreachAction = foreachAction;
        this.processorSupplier = processorSupplier;
    }

    @Override
    public void buildStreamTopology(KStreamBuilder kStreamBuilder) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Span> spanSerde = serdeFactory.createJsonProtoSpanSerde(application);
        final String fromTopic = kafkaConfigurationProvider.getFromTopic();
        final KStream<String, Span> stream = kStreamBuilder.stream(stringSerde, spanSerde, fromTopic);
        if (foreachAction != null) {
            stream.foreach(foreachAction);
        }
        if (processorSupplier != null) {
            stream.process(processorSupplier);
        }
    }

    /**
     * main() is an instance method because it is called by the static void *IsActiveController.main(String [] args)
     * methods; making it an instance method facilitates unit testing.
     */
    @Override
    public void main() {
        kafkaStreamStarter.createAndStartStream(this);
    }

}
