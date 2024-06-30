package com.aryak.kafka_stream.generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class GenericDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;
    private final Class<T> destinationClass;

    public GenericDeserializer(ObjectMapper objectMapper, Class<T> destinationClass) {
        this.objectMapper = objectMapper;
        this.destinationClass = destinationClass;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {

        try {
            return objectMapper.readValue(bytes , destinationClass);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
