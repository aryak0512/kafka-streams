package com.aryak.kafka_stream;

import com.aryak.kafka_stream.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.Properties;

import static java.util.stream.Collectors.toList;

@SpringBootApplication
@Slf4j
public class KafkaStreamApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

		// step 1 : define properties
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		// step 2 : create the topics to avoid errors
		createTopics(props, List.of("greetings", "greetings_uppercase"));

		// step 3 : get and start the topology
        var topology = GreetingsTopology.buildTopology();

        try(KafkaStreams kafkaStreams = new KafkaStreams(topology, props)){
            kafkaStreams.start();
        }
    }


    public void createTopics(Properties props, List<String> topics) {

        int partitions = 3;
        short replicationFactor = 1;
        AdminClient adminClient = AdminClient.create(props);

        var kafkaTopics = topics.stream().map(topic ->
                new NewTopic(topic, partitions, replicationFactor)
        ).collect(toList());

        var createTopicsResult = adminClient.createTopics(kafkaTopics);

        try {
            createTopicsResult.all().get();
			log.info("Topics created successfully!");
        } catch (Exception e) {
			log.error("Error in creating topics!", e);
        }

    }
}
