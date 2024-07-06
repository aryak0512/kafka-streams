package com.aryak.kafka_stream.controller;

import com.aryak.kafka_stream.domain.Product;
import com.aryak.kafka_stream.producer.ProducerService;
import com.aryak.kafka_stream.producer.ProducerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.aryak.kafka_stream.utils.Constants.PRODUCTS;

@RestController
@Slf4j
@RequestMapping(value = "/publish")
public class ProductController {

    private final ProducerService producerService;

    public ProductController(ProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * populate test data in kafka topic
     *
     * @return products published
     */
    @GetMapping(value = "/bulk/v1")
    public ResponseEntity<List<Product>> publishBulk() {

        var products = ProducerUtil.getProducts();

        products
                .parallelStream().forEach(p -> {
                    try {
                        producerService.produce(PRODUCTS, p);
                    } catch (Exception e) {
                        log.error("Exception occurred : {} ", e.getMessage(), e);
                    }
                });

        log.info("Products published successfully!");
        return new ResponseEntity<>(products, HttpStatus.OK);
    }

    @GetMapping(value = "/v1/{key}")
    public ResponseEntity<List<Product>> publish(@PathVariable(value = "key") String key) {

        var products = ProducerUtil.getProducts();
        products
                .parallelStream().forEach(p -> {
                    try {
                        producerService.produce(PRODUCTS, key, p);
                    } catch (Exception e) {
                        log.error("Exception occurred : {} ", e.getMessage(), e);
                    }
                });

        log.info("Product published successfully with key : {} !", key);
        return new ResponseEntity<>(products, HttpStatus.OK);
    }

    @GetMapping(value = "/v1")
    public ResponseEntity<List<Product>> publish() {

        var products = ProducerUtil.getProducts();
        products
                .parallelStream().forEach(p -> {
                    try {
                        producerService.produce(PRODUCTS, null, p);
                    } catch (Exception e) {
                        log.error("Exception occurred : {} ", e.getMessage(), e);
                    }
                });

        log.info("Product published successfully!");
        return new ResponseEntity<>(products, HttpStatus.OK);
    }

    /**
     * Body passed through API
     *
     * @param key
     * @param product
     * @return the product produced
     */
    @PostMapping(value = "/v1/{key}")
    public ResponseEntity<Product> publishBody(@PathVariable(value = "key", required = false) String key,
                                               @RequestBody Product product) {
        try {
            producerService.produce(PRODUCTS, key, product);
        } catch (Exception e) {
            log.error("Exception occurred : {} ", e.getMessage(), e);
        }
        log.info("Product : {} published successfully!", product);
        return new ResponseEntity<>(product, HttpStatus.OK);
    }
}
