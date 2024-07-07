package com.aryak.kafka_stream.controller;

import com.aryak.kafka_stream.domain.Order;
import com.aryak.kafka_stream.domain.Product;
import com.aryak.kafka_stream.service.OrderService;
import com.aryak.kafka_stream.service.ProducerService;
import com.aryak.kafka_stream.producer.ProducerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.aryak.kafka_stream.utils.Constants.PRODUCTS;

@RestController
@Slf4j
@RequestMapping(value = "/publish")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * populate test data in kafka topic
     *
     * @return products published
     */
    @GetMapping(value = "/bulk/v1")
    public ResponseEntity<List<Order>> publishBulk() {

        var products = ProducerUtil.getOrders();

        products
                .parallelStream().forEach(p -> {
                    try {
                        orderService.produce(PRODUCTS, p);
                    } catch (Exception e) {
                        log.error("Exception occurred : {} ", e.getMessage(), e);
                    }
                });

        log.info("Products published successfully!");
        return new ResponseEntity<>(products, HttpStatus.OK);
    }

    @GetMapping(value = "/v1/{key}")
    public ResponseEntity<List<Order>> publish(@PathVariable(value = "key") String key) {

        var products = ProducerUtil.getOrders();
        products
                .parallelStream().forEach(p -> {
                    try {
                        orderService.produce(PRODUCTS, key, p);
                    } catch (Exception e) {
                        log.error("Exception occurred : {} ", e.getMessage(), e);
                    }
                });

        log.info("Product published successfully with key : {} !", key);
        return new ResponseEntity<>(products, HttpStatus.OK);
    }

    @GetMapping(value = "/v1")
    public ResponseEntity<List<Order>> publish() {

        var products = ProducerUtil.getOrders();
        products
                .parallelStream().forEach(p -> {
                    try {
                        orderService.produce(PRODUCTS, null, p);
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
     * @param order
     * @return the product produced
     */
    @PostMapping(value = "/v1/{key}")
    public ResponseEntity<Order> publishBody(@PathVariable(value = "key", required = false) String key,
                                               @RequestBody Order order) {
        try {
            orderService.produce(PRODUCTS, key, order);
        } catch (Exception e) {
            log.error("Exception occurred : {} ", e.getMessage(), e);
        }
        log.info("Product : {} published successfully!", order);
        return new ResponseEntity<>(order, HttpStatus.OK);
    }
}
