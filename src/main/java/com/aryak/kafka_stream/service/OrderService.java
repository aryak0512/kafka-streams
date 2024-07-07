package com.aryak.kafka_stream.service;

import com.aryak.kafka_stream.domain.Order;
import com.aryak.kafka_stream.domain.Product;

import java.util.List;

public interface OrderService {

    void produce(String topic, Order product) throws Exception;

    void produce(String topic, List<Order> products) throws Exception;

    void produce(String topic, String key, Order product) throws Exception;
}
