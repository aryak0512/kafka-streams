package com.aryak.kafka_stream.producer;

import com.aryak.kafka_stream.domain.Product;

import java.util.List;

public interface ProducerService {

    void produce(String topic, Product product) throws Exception;

    void produce(String topic, List<Product> products) throws Exception;

    void produce(String topic, String key, Product product) throws Exception;
}
