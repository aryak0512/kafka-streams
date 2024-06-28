package com.aryak.kafka_stream;

import com.aryak.kafka_stream.topology.GreetingsTopology;
import com.aryak.kafka_stream.utils.KafkaUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

import static com.aryak.kafka_stream.utils.Constants.*;

@SpringBootApplication
public class KafkaStreamApplication implements CommandLineRunner {

    private final KafkaUtils kafkaUtils;

    public KafkaStreamApplication(KafkaUtils kafkaUtils) {
        this.kafkaUtils = kafkaUtils;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        // step 1 : fetch properties
        var props = kafkaUtils.getProperties();

        // step 2 : create the topics to avoid errors
        kafkaUtils.createTopics(props, List.of(GREETINGS, GREETINGS_UPPERCASE, RESULT_TOPIC));

        // step 3 : get and start the topology
        var topology = GreetingsTopology.buildTopology2();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        // graceful application shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try {
            kafkaStreams.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
