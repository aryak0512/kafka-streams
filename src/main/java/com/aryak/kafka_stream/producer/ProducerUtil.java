package com.aryak.kafka_stream.producer;

import com.aryak.kafka_stream.domain.Product;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;


/**
 * @author aryak
 * A utility bean to produce test data into kafka topic
 */

@Slf4j
public class ProducerUtil {

    /**
     * configure the producer properties
     *
     * @return the producer properties
     */
    public static Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }


    /**
     * Populate dummy product list to push into kafka topics
     *
     * @return List of product
     */
    public static List<Product> getProducts() {

        var p1 = Product.builder()
                .createdAt(LocalDateTime.now())
                .productId(11213)
                .productName("Product 1")
                .build();

        var p2 = Product.builder()
                .createdAt(LocalDateTime.now())
                .productId(535353)
                .productName("Product 2")
                .build();

        var p3 = Product.builder()
                .createdAt(LocalDateTime.now())
                .productId(657577)
                .productName("Product 3")
                .build();

        return List.of(p1, p2, p3);
    }
}
