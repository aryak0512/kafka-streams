package com.aryak.kafka_stream.service.impl;

import com.aryak.kafka_stream.service.StateStoreService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Optional;
@Slf4j
@Service
public class StateStoreServiceImpl implements StateStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public StateStoreServiceImpl(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @Override
    public ReadOnlyKeyValueStore<String, Long> get(String storeName) {

        Optional<KafkaStreams> kafkaStreams = Optional.ofNullable(streamsBuilderFactoryBean.getKafkaStreams());
        if ( kafkaStreams.isPresent() ) {

            return kafkaStreams.get()
                    .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
        }
        log.error("Stream value is null!");
        return null;
    }
}
