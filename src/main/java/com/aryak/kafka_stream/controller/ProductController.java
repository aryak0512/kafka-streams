package com.aryak.kafka_stream.controller;

import com.aryak.kafka_stream.domain.Product;
import com.aryak.kafka_stream.producer.ProducerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Slf4j
public class ProductController {

    private final ProducerUtil producerUtil;

    public ProductController(ProducerUtil producerUtil) {
        this.producerUtil = producerUtil;
    }

    /**
     * populate test data in kafka topic
     * @return products published
     */
    @GetMapping(value = "/publish/v1")
    public ResponseEntity<List<Product>> publish() {

        var products = producerUtil.getProducts();
        products
                .parallelStream().forEach(p -> {
                    try {
                        producerUtil.publish(p);
                    } catch (Exception e) {
                        log.error("Exception occurred : {} ", e.getMessage(), e);
                    }
                });

        log.info("Products published successfully!");
        return new ResponseEntity<>(products, HttpStatus.OK);
    }
}
