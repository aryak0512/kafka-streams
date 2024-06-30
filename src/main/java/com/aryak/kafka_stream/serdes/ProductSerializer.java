package com.aryak.kafka_stream.serdes;

import com.aryak.kafka_stream.domain.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class ProductSerializer implements Serializer<Product> {

    private final ObjectMapper objectMapper;

    public ProductSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String s, Product product) {

        try {
            return objectMapper.writeValueAsBytes(product);
        } catch (Exception e) {
            log.error("Exception occurred : {}", e.getMessage(), e);
        }

        return new byte[0];
    }
}
