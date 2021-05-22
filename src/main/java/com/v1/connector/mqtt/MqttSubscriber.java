package com.v1.connector.mqtt;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttSubscriber implements MqttCallback {


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
        System.out.println("App is yet to start");
        try {
            client.subscribe("test/topics");
            client.setCallback(this);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("Connection Lost");
        cause.printStackTrace();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        println("v1.0 Topic ".concat(topic));
        println("Message :: ".concat(new String(message.getPayload())));
        mqttQueue.add(new String(message.getPayload()));
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        System.out.println("Delivery completed " + token.getMessageId());
    }

    public void println(Object message) {
        System.out.println(message);
    }

    public BlockingQueue<String> getMqttQueue() {
        return mqttQueue;
    }
}
