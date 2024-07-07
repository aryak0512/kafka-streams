package com.aryak.kafka_stream.domain;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@Builder
public record AggregateResult(
        String key,
        Set<String> values,
        int runningCount) {

    public AggregateResult() {
        this("", new HashSet<>(), 0);
    }
}
