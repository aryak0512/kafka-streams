package com.aryak.kafka_stream.serdes;

import com.aryak.kafka_stream.domain.Product;
import com.aryak.kafka_stream.generic.GenericDeserializer;
import com.aryak.kafka_stream.generic.GenericSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {

    private SerdeFactory() {
    }

    public static Serde<Product> productSerde() {
        return new ProductSerde();
    }

    /**
     * building the generic serde
     * @return a product serde
     */
    public static Serde<Product> productSerdeUsingGeneric() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);
        GenericSerializer<Product> serializer = new GenericSerializer<>(objectMapper);
        GenericDeserializer<Product> deserializer = new GenericDeserializer<>(objectMapper, Product.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
