package com.aryak.kafka_stream.service;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public interface StateStoreService {

    ReadOnlyKeyValueStore<String, Long> get(String storeName);
}
