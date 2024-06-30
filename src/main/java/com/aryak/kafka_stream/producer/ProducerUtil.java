package com.aryak.kafka_stream.producer;

import com.aryak.kafka_stream.domain.Product;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.aryak.kafka_stream.utils.Constants.PRODUCTS;

/**
 * @author aryak
 * A utility bean to produce test data into kafka topic
 */
@Component
@Slf4j
public class ProducerUtil {

    private final ObjectMapper objectMapper;

    public ProducerUtil(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());

    /**
     * configure the producer properties
     * @return the producer properties
     */
    public Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    /**
     * Convert to JSON and send to kafka
     * @param product the data to be sent
     * @throws JsonProcessingException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @SneakyThrows
    public void publish(Product product) throws Exception {
        String json = objectMapper.writeValueAsString(product);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(PRODUCTS, null, json);
        var send = producer.send(producerRecord);
        var recordMetadata = send.get();
        log.info("Publish success | Offset : {} | Partition : {}", recordMetadata.offset(), recordMetadata.partition());
    }

    /**
     * Populate dummy product list to push into kafka topics
     * @return List of product
     */
    public List<Product> getProducts(){

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
