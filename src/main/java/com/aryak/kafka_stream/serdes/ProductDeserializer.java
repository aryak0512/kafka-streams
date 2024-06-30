package com.aryak.kafka_stream.serdes;

import com.aryak.kafka_stream.domain.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class ProductDeserializer implements Deserializer<Product> {

    private final ObjectMapper objectMapper;

    public ProductDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Product deserialize(String s, byte[] bytes) {

        try {
            return objectMapper.readValue(bytes, Product.class);
        } catch (Exception e) {
            log.error("Exception occurred : {}", e.getMessage(), e);
        }
        return null;
    }
}
