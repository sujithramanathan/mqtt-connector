package com.v1.connector.task;

import com.v1.connector.mqtt.MqttSubscriber;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.client.mqttv3.MqttException;

import javax.sound.sampled.Line;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class MqttSourceTask extends SourceTask {

    private MqttSubscriber subscriber;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        System.out.println("Started source");
        System.out.println("Props is " + props);
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
        System.out.println("Stopping Kafka Connector");
    }

}
