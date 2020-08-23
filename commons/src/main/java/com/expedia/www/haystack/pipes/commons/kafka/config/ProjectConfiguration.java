package com.expedia.www.haystack.pipes.commons.kafka.config;

import com.expedia.www.haystack.commons.config.ConfigurationLoader;
import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;

public class ProjectConfiguration {

    private final Config haystackConfig;
    private static ProjectConfiguration projectConfiguration = null;

    private ProjectConfiguration() {
        String resourceName = System.getenv("configFilePath") == null ? "config/base.conf" : System.getenv("configFilePath");
        Config config = ConfigurationLoader.loadConfigFileWithEnvOverrides(resourceName, "HAYSTACK_PROP_");
        haystackConfig = config.getConfig("haystack");
    }

    public static ProjectConfiguration getInstance(){
        if(null == projectConfiguration){
            projectConfiguration = new ProjectConfiguration();
        }
        return projectConfiguration;
    }

    public KafkaConsumerConfig getKafkaConsumerConfig() {
        Config kafkaConfig = haystackConfig.getConfig("kafka");
        return new KafkaConsumerConfig(kafkaConfig.getString("brokers"),
                kafkaConfig.getInt("port"), kafkaConfig.getString("fromtopic"),
                kafkaConfig.getString("totopic"), kafkaConfig.getInt("threadcount"),
                kafkaConfig.getInt("sessiontimeout"), kafkaConfig.getInt("maxwakeups"),
                kafkaConfig.getInt("wakeuptimeoutms"), kafkaConfig.getLong("polltimeoutms"),
                kafkaConfig.getLong("commitms"));
    }

    public KafkaProducerConfig getKafkaProducerConfig() {
        Config kafkaConfig = haystackConfig.getConfig("externalkafka");
        return new KafkaProducerConfig(kafkaConfig.getString("brokers"),
                kafkaConfig.getInt("port"), kafkaConfig.getString("totopic"),
                kafkaConfig.getString("acks"), kafkaConfig.getInt("batchsize"),
                kafkaConfig.getInt("lingerms"), kafkaConfig.getInt("buffermemory"));
    }

    public FirehoseConfig getFirehoseConfig() {
        Config firehoseConfig = haystackConfig.getConfig("firehose");
        return new FirehoseConfig(firehoseConfig.getString("url"), firehoseConfig.getString("streamname"),
                firehoseConfig.getString("signingregion"), firehoseConfig.getInt("initialretrysleep"),
                firehoseConfig.getInt("maxretrysleep"), firehoseConfig.getBoolean("usestringbuffering"),
                firehoseConfig.getInt("maxbatchinterval"), firehoseConfig.getInt("maxparallelismpershard"));
    }

    public PipesConfig getPipesConfig() {
        Config pipesConfig = haystackConfig.getConfig("pipe");
        return new PipesConfig(pipesConfig.getConfig("streams").getInt("replicationfactor"));
    }

    public HttpPostConfig getHttpPostConfig() {
        Config httpPostConfig = haystackConfig.getConfig("httppost");
        Map<String, String> headers = new HashMap<>();
        for (Config header : httpPostConfig.getConfigList("headers")) {
            headers.put(header.getString("name"), header.getString("value"));
        }
        return new HttpPostConfig(httpPostConfig.getString("maxbytes"),
                httpPostConfig.getString("url"), httpPostConfig.getString("bodyprefix"),
                httpPostConfig.getString("bodysuffix"), httpPostConfig.getString("separator"),
                headers, httpPostConfig.getString("pollpercent"));
    }
}
