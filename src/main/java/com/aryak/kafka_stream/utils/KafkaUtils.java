package com.aryak.kafka_stream.utils;

import com.aryak.kafka_stream.handler.DeserializationHandler;
import com.aryak.kafka_stream.handler.SerializationHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.function.IntSupplier;
import java.util.function.Supplier;


/**
 * @author aryak
 *
 * @apiNote Bean responsible for performing boiler plate kafka operations
 */
@Component
@Slf4j
public class KafkaUtils {

    /**
     * Sets the basic kafka broker properties
     * @return Properties
     */
    public Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, getCores.getAsInt());
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, SerializationHandler.class.getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DeserializationHandler.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return props;
    }

    /**
     * Returns the no. of CPU cores of the machine
     */
    private final IntSupplier getCores = () -> Runtime.getRuntime().availableProcessors();

    /**
     * Takes kafka server properties & creates the topics provided in topics list
     * @param props the props required to create admin client
     * @param topics list of topic strings to be created
     * @apiNote Programmatically creates topics on the kafka cluster
     */
    public void createTopics(Properties props, List<String> topics) {

        int partitions = 3;
        short replicationFactor = 1;
        AdminClient adminClient = AdminClient.create(props);

        // building the actual topic object
        var kafkaTopics = topics
                .stream()
                .map(topic -> new NewTopic(topic, partitions, replicationFactor))
                .toList();

        var createTopicsResult = adminClient.createTopics(kafkaTopics);

        try {
            // test if topics created successfully
            createTopicsResult.all().get();
            log.info("Topics created successfully!");
        } catch (Exception e) {
            log.error("Error in creating topics!", e);
        }

    }
}
