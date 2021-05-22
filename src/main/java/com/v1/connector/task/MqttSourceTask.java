package com.v1.connector.task;

import com.v1.connector.mqtt.MqttSubscriber;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.log4j.Logger;


import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;




public class MqttSourceTask extends SourceTask {

    private static final Logger logger = Logger.getLogger(MqttSourceTask.class);

    private MqttSubscriber subscriber;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        logger.debug("Started source");
        logger.debug("Props is " + props);
        this.subscriber = new MqttSubscriber();
        this.subscriber.start();
    }

    @Override
    public List<SourceRecord> poll() {

        while (true) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return createRecords();
        }

    }


    private List<SourceRecord> createRecords() {

        List<SourceRecord> sourceRecordList = new ArrayList<>();
        BlockingQueue<String> mqttQueue = subscriber.getMqttQueue();

        try {
            while (!mqttQueue.isEmpty()) {
                sourceRecordList.add(new SourceRecord(null, null, "tvm-test-topic", null,
                        Schema.STRING_SCHEMA, "mqttTopic", Schema.STRING_SCHEMA, mqttQueue.take() + " - " + System.currentTimeMillis()));
            }
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

        return sourceRecordList;
    }

    @Override
    public void stop() {
        logger.debug("Stopping Kafka Connector");
    }

}
