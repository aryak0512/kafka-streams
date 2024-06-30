package com.aryak.kafka_stream.serdes;

import com.aryak.kafka_stream.domain.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author aryak
 * @apiNote The serde for the domain object.
 *
 * The custom serializer & deserializer are plugged in to the serde using this bean
 */
public class ProductSerde implements Serde<Product> {

    ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);

    @Override
    public Serializer<Product> serializer() {
        return new ProductSerializer(objectMapper);
    }

    @Override
    public Deserializer<Product> deserializer() {
        return new ProductDeserializer(objectMapper);
    }
}
