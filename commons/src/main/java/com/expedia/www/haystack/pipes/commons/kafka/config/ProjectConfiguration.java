package com.expedia.www.haystack.pipes.commons.kafka.config;

import com.expedia.www.haystack.commons.config.ConfigurationLoader;
import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectConfiguration {

    private final Config haystackConfig;
    private static ProjectConfiguration projectConfiguration = null;
    private static KafkaConsumerConfig kafkaConsumerConfig = null;
    private static KafkaProducerConfig kafkaProducerConfig = null;
    private static FirehoseConfig firehoseConfig = null;
    private static PipesConfig pipesConfig = null;
    private static HttpPostConfig httpPostConfig = null;

    private ProjectConfiguration() {
        String resourceName = System.getenv("configFilePath") == null ? "config/base.conf" : System.getenv("configFilePath");
        Config config = ConfigurationLoader.loadConfigFileWithEnvOverrides(resourceName, "HAYSTACK_PROP_");
        haystackConfig = config.getConfig("haystack");
    }

    public static ProjectConfiguration getInstance() {
        if (null == projectConfiguration) {
            projectConfiguration = new ProjectConfiguration();
        }
        return projectConfiguration;
    }

    public KafkaConsumerConfig getKafkaConsumerConfig() {
        if (null == kafkaConsumerConfig) {
            Config kafkaConfig = haystackConfig.getConfig("kafka");
            kafkaConsumerConfig = new KafkaConsumerConfig(kafkaConfig.getString("brokers"),
                    kafkaConfig.getInt("port"), kafkaConfig.getString("fromtopic"),
                    kafkaConfig.getString("totopic"), kafkaConfig.getInt("threadcount"),
                    kafkaConfig.getInt("sessiontimeout"), kafkaConfig.getInt("maxwakeups"),
                    kafkaConfig.getInt("wakeuptimeoutms"), kafkaConfig.getLong("polltimeoutms"),
                    kafkaConfig.getLong("commitms"));
        }
        return kafkaConsumerConfig;
    }

    public KafkaProducerConfig getKafkaProducerConfig() {
        if (null == kafkaProducerConfig) {
            Config kafkaConfig = haystackConfig.getConfig("externalkafka");
            Map<String, Config> extractorConfigMap = new HashMap<>();
            List<Config> extractorConfigs = (List<Config>) kafkaConfig.getConfigList("extractors");
            extractorConfigs.forEach(extractorConfig -> {
                String name = extractorConfig.getString("name");
                Config config = extractorConfig.getConfig("config");
                extractorConfigMap.put(name, config);
            });
            SpanKeyExtractorConfig spanKeyExtractorConfig = new SpanKeyExtractorConfig(extractorConfigMap);
            kafkaProducerConfig = new KafkaProducerConfig(kafkaConfig.getString("brokers"),
                    kafkaConfig.getInt("port"), kafkaConfig.getString("totopic"),
                    kafkaConfig.getString("acks"), kafkaConfig.getInt("batchsize"),
                    kafkaConfig.getInt("lingerms"), kafkaConfig.getInt("buffermemory"),
                    spanKeyExtractorConfig);
        }
        return kafkaProducerConfig;
    }

    public FirehoseConfig getFirehoseConfig() {
        if (null == firehoseConfig) {
            Config firehoseConf = haystackConfig.getConfig("firehose");
            firehoseConfig = new FirehoseConfig(firehoseConf.getString("url"), firehoseConf.getString("streamname"),
                    firehoseConf.getString("signingregion"), firehoseConf.getInt("initialretrysleep"),
                    firehoseConf.getInt("maxretrysleep"), firehoseConf.getBoolean("usestringbuffering"),
                    firehoseConf.getInt("maxbatchinterval"), firehoseConf.getInt("maxparallelismpershard"));

        }
        return firehoseConfig;
    }

    public PipesConfig getPipesConfig() {
        if (null == pipesConfig) {
            Config pipesConf = haystackConfig.getConfig("pipe");
            pipesConfig = new PipesConfig(pipesConf.getConfig("streams").getInt("replicationfactor"));
        }
        return pipesConfig;
    }

    public HttpPostConfig getHttpPostConfig() {
        if (null == httpPostConfig) {
            Config httpPostConf = haystackConfig.getConfig("httppost");
            Map<String, String> headers = new HashMap<>();
            for (Config header : httpPostConf.getConfigList("headers")) {
                headers.put(header.getString("name"), header.getString("value"));
            }
            httpPostConfig = new HttpPostConfig(httpPostConf.getString("maxbytes"),
                    httpPostConf.getString("url"), httpPostConf.getString("bodyprefix"),
                    httpPostConf.getString("bodysuffix"), httpPostConf.getString("separator"),
                    headers, httpPostConf.getString("pollpercent"));
        }
        return httpPostConfig;
    }

}
