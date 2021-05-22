package com.v1.connector.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MqttSourceConfig extends AbstractConfig {

    private static final Logger logger = LoggerFactory.getLogger(MqttSourceConfig.class.getName());

    public static ConfigDef configDef = new ConfigDef();

    public MqttSourceConfig(Map<String, String> configMap){
        super(configDef, configMap);
    }


}
