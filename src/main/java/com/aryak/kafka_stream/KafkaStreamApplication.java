package com.aryak.kafka_stream;

import com.aryak.kafka_stream.domain.Product;
import com.aryak.kafka_stream.handler.ProcessHandler;
import com.aryak.kafka_stream.producer.ProducerUtil;
import com.aryak.kafka_stream.topology.TopologyFactory;
import com.aryak.kafka_stream.utils.KafkaUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.*;
import java.util.stream.StreamSupport;

import static com.aryak.kafka_stream.utils.Constants.*;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamApplication.class);
    private final KafkaUtils kafkaUtils;

    public KafkaStreamApplication(KafkaUtils kafkaUtils) {
        this.kafkaUtils = kafkaUtils;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamApplication.class, args);
    }

    /**
     * configure the producer properties
     *
     * @return the producer properties
     */
    @Bean(value = "properties")
    public static Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean(value = "properties2")
    public static Properties getProperties2() {
        Properties props = new Properties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        return props;
    }

    @Override
    public void run(String... args) throws Exception {

        play();

        // step 1 : fetch properties
        var props = kafkaUtils.getProperties();

        // step 2 : create the topics to avoid errors
        kafkaUtils.createTopics(props, List.of(
                GREETINGS,
                GREETINGS_UPPERCASE,
                RESULT_TOPIC,
                PRODUCTS,
                PRODUCTS_TRANSFORMED,
                ORDERS,
                GENERAL_ORDERS,
                RESTAURANT_ORDERS,
                BOOKS,
                AUTHORS,
                BOOK_INFO
        ));

        // step 3 : get and start the topology
        var topology = TopologyFactory.buildTopology13();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.setUncaughtExceptionHandler(new ProcessHandler());

        // graceful application shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook executed.");
            kafkaStreams.close();
        }));

        try {
            kafkaStreams.start();
        } catch (Exception e) {
            log.error("Exception occurred : {}", e.getMessage(), e);
        }

    }

    /**
     * demonstration of spliterator operation
     */
    private void play() {

        List<Product> products = ProducerUtil.getProducts();
        Iterator<Product> iterator = products.iterator();
        Spliterator<Product> productSpliterator = Spliterators.spliteratorUnknownSize(iterator, 0);

        List<String> productNames = StreamSupport.stream(productSpliterator, false)
                .map(Product::productName)
                .toList();

        log.info("Product names only : {} ", productNames);
    }
}
