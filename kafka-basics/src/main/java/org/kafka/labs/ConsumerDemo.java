package org.kafka.labs;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Staring Kafka Consumer !!");
        // Create Consumer Properties
        String bootStrapServer = "127.0.0.1:9092";
        String topic = "cities";
        String groupId = "javacitygrp";

        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // Subscribe to a kafka topic or topics
        consumer.subscribe(Collections.singleton(topic));

        // fetch data(s) and process until  exit
        while (true){
            log.info("Start fetching data !!");
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(3000));
            // Process the data
            for(ConsumerRecord<String, String> record: records){
                log.info("Topic : " + topic + ", Partition : " + record.partition() + ", Offset : " + record.offset());
                log.info("Key : " + record.key() + ", Value : " + record.value());
            }
        }

        // close Consumer
    }
}
