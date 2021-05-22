package com.v1.connector.mqtt;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttSubscriber implements MqttCallback {


    private static final Logger logger = Logger.getLogger(MqttSubscriber.class);

    private MqttClient client;
    private BlockingQueue<String> mqttQueue = null;

    public MqttSubscriber() {
        init();
    }

    public void init() {
        try {
            client = new MqttClient("tcp://localhost:1883", UUID.randomUUID().toString(), null);
            client.connect();
            mqttQueue = new LinkedBlockingDeque<>();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        logger.debug("App is yet to start");
        try {
            client.subscribe("test/topics");
            client.setCallback(this);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void connectionLost(Throwable cause) {
        logger.debug("Connection Lost");
        cause.printStackTrace();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        logger.debug("v1.0 Topic ".concat(topic));
        logger.debug("Message :: ".concat(new String(message.getPayload())));
        mqttQueue.add(new String(message.getPayload()));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        logger.debug("Delivery completed " + token.getMessageId());
    }


    public BlockingQueue<String> getMqttQueue() {
        return mqttQueue;
    }
}
