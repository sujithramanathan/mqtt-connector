package com.v1.connector;

import com.v1.connector.config.MqttSourceConfig;
import com.v1.connector.mqtt.MqttSubscriber;
import com.v1.connector.task.MqttSourceTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MqttSourceConnector extends SourceConnector {

    private Map<String, String> props;
    private MqttSourceConfig sourceConfig;

    @Override
    public void start(Map<String, String> props) {
        System.out.println("MqttSourceConnector props "+props);
        this.props = props;
        sourceConfig = new MqttSourceConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(1);
        Map<String, String> taskProps = new HashMap<>(props);
        taskConfigs.add(taskProps);
        return taskConfigs;
    }

    @Override
    public void stop() {
        System.out.println("MqttSourceConnector will be stopped");
    }

    @Override
    public ConfigDef config() {
        return MqttSourceConfig.configDef;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
