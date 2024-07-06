package com.aryak.kafka_stream.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

@Slf4j
public class DeserializationHandler implements DeserializationExceptionHandler {

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {
        String value = new String(consumerRecord.value());
        String key = new String(consumerRecord.key());
        int partition = consumerRecord.partition();
        String topic = consumerRecord.topic();
        log.info("Exception occurred in deserialization : {} | Record - K : {} | V : {} | P : {} | T : {}", e.getMessage(), key, value, partition, topic, e);
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {
        throw new UnsupportedOperationException();
    }
}
