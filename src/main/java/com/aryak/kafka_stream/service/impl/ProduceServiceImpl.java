package com.aryak.kafka_stream.service.impl;

import com.aryak.kafka_stream.domain.Product;
import com.aryak.kafka_stream.service.ProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

import static com.aryak.kafka_stream.utils.Constants.PRODUCTS;

@Slf4j
@Service
public class ProduceServiceImpl implements ProducerService {

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;

    public ProduceServiceImpl(Properties properties, ObjectMapper mapper) {
        this.producer = new KafkaProducer<>(properties);
        this.mapper = mapper;
    }

    @Override
    public void produce(String topic, Product product) throws Exception {
        String json = mapper.writeValueAsString(product);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, product.productName(), json);
        // sync and blocking
        var recordMetadata = producer.send(producerRecord).get();
        log.info("Publish success | Offset : {} | Partition : {}", recordMetadata.offset(), recordMetadata.partition());
    }

    @Override
    public void produce(String topic, List<Product> products) throws Exception {
        // not required at the moment
    }

    @Override
    public void produce(String topic, String key, Product product) throws Exception {
        String json = mapper.writeValueAsString(product);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, json);
        // sync and blocking
        var recordMetadata = producer.send(producerRecord).get();
        log.info("Publish success | Offset : {} | Partition : {}", recordMetadata.offset(), recordMetadata.partition());
    }
}
