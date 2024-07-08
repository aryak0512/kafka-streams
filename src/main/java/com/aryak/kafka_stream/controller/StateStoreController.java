package com.aryak.kafka_stream.controller;

import com.aryak.kafka_stream.domain.ResultDto;
import com.aryak.kafka_stream.service.StateStoreService;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static com.aryak.kafka_stream.utils.Constants.GENERAL_ORDERS_COUNT;

@RestController
@RequestMapping(value = "/state")
public class StateStoreController {

    private final StateStoreService stateStoreService;

    public StateStoreController(StateStoreService stateStoreService) {
        this.stateStoreService = stateStoreService;
    }

    @GetMapping(value = "/count/general")
    public List<ResultDto> getStore(){

        ReadOnlyKeyValueStore<String, Long> keyValueStore = stateStoreService.get(GENERAL_ORDERS_COUNT);
        KeyValueIterator<String, Long> all = keyValueStore.all();
        Spliterator<KeyValue<String, Long>> spliterator = Spliterators.spliteratorUnknownSize(all, 0);

        return StreamSupport.stream(spliterator, false)
                .map(e -> new ResultDto(e.key, e.value))
                .toList();
    }

}
