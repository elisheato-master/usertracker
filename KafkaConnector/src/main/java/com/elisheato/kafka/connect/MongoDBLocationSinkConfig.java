// MongoDBLocationSinkConfig.java
package com.example.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MongoDBLocationSinkConfig extends AbstractConfig {

    public static final String MONGO_URI_CONFIG = "mongodb.uri";
    public static final String MONGO_URI_DOC = "MongoDB connection URI including credentials";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MONGO_URI_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, MONGO_URI_DOC);

    public MongoDBLocationSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}