package com.aryak.kafka_stream.controller;

import com.aryak.kafka_stream.domain.Order;
import com.aryak.kafka_stream.producer.ProducerUtil;
import com.aryak.kafka_stream.service.impl.GenericProducerImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.aryak.kafka_stream.utils.Constants.*;

@RestController
@Slf4j
@RequestMapping(value = "/book")
public class BookController {

    private final GenericProducerImpl genericProducer;

    public BookController(GenericProducerImpl genericProducer) {
        this.genericProducer = genericProducer;
    }

    /**
     * populate test data in kafka topic
     *
     * @return products published
     */
    @GetMapping
    public ResponseEntity<List<Order>> publish() {
        var books = ProducerUtil.getBooks();
        var authors = ProducerUtil.getAuthors();

        // publish all books
        books.parallelStream().forEach(b -> {
            try {
                genericProducer.produceBook(BOOKS, b);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // publish all authors
        authors.parallelStream().forEach(a -> {
            try {
                genericProducer.produceAuthor(AUTHORS, a);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return new ResponseEntity<>(null, HttpStatus.OK);
    }

}
