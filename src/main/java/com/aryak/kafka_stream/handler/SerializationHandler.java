package com.aryak.kafka_stream.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;
@Slf4j
public class SerializationHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> producerRecord, Exception e) {
        String value = new String(producerRecord.value());
        String key = new String(producerRecord.key());
        int partition = producerRecord.partition();
        String topic = producerRecord.topic();
        log.info("Exception occurred in serialization : {} | Record - K : {} | V : {} | P : {} | T : {}", e.getMessage(), key, value, partition, topic, e);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {
        // no need to add anything here
    }
}
