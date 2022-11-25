package org.kafka.sample;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    KafkaProducer<String, String> kafkaProducer;
    String kafkaTopic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String kafkaTopic){
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
        log.info("Producer is closed !!");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, messageEvent.getData()));
        log.info("kafka message is sent : " +  messageEvent.getData());
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {

    }
}
