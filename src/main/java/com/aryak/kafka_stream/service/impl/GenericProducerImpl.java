package com.aryak.kafka_stream.service.impl;

import com.aryak.kafka_stream.domain.Author;
import com.aryak.kafka_stream.domain.Book;
import com.aryak.kafka_stream.domain.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonAppend;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@Slf4j
public class GenericProducerImpl {

    private final Properties props;
    private final ObjectMapper mapper;

    public GenericProducerImpl(Properties properties2, ObjectMapper mapper) {
        this.props = properties2;
        this.mapper = mapper;
    }

    /**
     * method that produces books into the books topic with key = authorId and value = book json
     * @param topic
     * @param book
     * @throws Exception
     */
    public void produceBook(String topic, Book book) throws Exception {
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        String json = mapper.writeValueAsString(book);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, book.authorId(), json);
        var recordMetadata = producer.send(producerRecord).get();
        log.info("Publish success | Offset : {} | Partition : {}", recordMetadata.offset(), recordMetadata.partition());
    }

    /**
     * method that produces authors into the authors topic with key = id and value = author json
     * @param topic
     * @param author
     * @throws Exception
     */
    public void produceAuthor(String topic, Author author) throws Exception {
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        String json = mapper.writeValueAsString(author);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, author.id(), json);
        var recordMetadata = producer.send(producerRecord).get();
        log.info("Publish success | Offset : {} | Partition : {}", recordMetadata.offset(), recordMetadata.partition());
    }

}
