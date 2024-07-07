package com.aryak.kafka_stream.serdes;

import com.aryak.kafka_stream.domain.AggregateResult;
import com.aryak.kafka_stream.domain.AggregateRevenue;
import com.aryak.kafka_stream.domain.Order;
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

    /**
     * building the AggregateResult serde
     * @return a AggregateResult serde
     */
    public static Serde<AggregateResult> aggregateResultSerde() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);
        GenericSerializer<AggregateResult> serializer = new GenericSerializer<>(objectMapper);
        GenericDeserializer<AggregateResult> deserializer = new GenericDeserializer<>(objectMapper, AggregateResult.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<Order> orderSerdeUsingGeneric() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);
        GenericSerializer<Order> serializer = new GenericSerializer<>(objectMapper);
        GenericDeserializer<Order> deserializer = new GenericDeserializer<>(objectMapper, Order.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<AggregateRevenue> aggregateRevenueSerde() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, false);
        GenericSerializer<AggregateRevenue> serializer = new GenericSerializer<>(objectMapper);
        GenericDeserializer<AggregateRevenue> deserializer = new GenericDeserializer<>(objectMapper, AggregateRevenue.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
