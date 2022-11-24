package org.kafka.labs;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Start Kafka Producer !!");

        String bootStrapServer = "127.0.0.1:9092";
        String topic = "cities";
        // Create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(10));
        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // Create a producer record
        for (int i=0; i<10; i++) {

            String key = "ilkod_"  + i;
            String value = "iladi_kafka_" + i;

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            // Send the data    -- asyncronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null){
                        log.info("Received message is :" + "\n" +
                                "Topic : " + metadata.topic() + "\n" +
                                "Partition : " + metadata.partition() + "\n" +
                                "Offset : " + metadata.offset() + "\n" +
                                "Timestamp : " + metadata.timestamp() + "\n" +
                                "Key : " + producerRecord.key() +  "\n" +
                                "Value : " + producerRecord.value()
                        );
                    } else {
                        log.error("An error occured", e);
                    }
                }
            });
            log.info("Kafka message is sent");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // Flush and close the producer
        // Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
        // greater than 0) and blocks on the completion of the requests associated with these records.
        // A request is considered completed when it is successfully acknowledged
        producer.flush();
        // This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
        // If the producer is unable to complete all requests before the timeout expires, this method will fail
        // any unsent and unacknowledged records immediately.
        producer.close();
        log.info("Producer is closed");
    }
}
