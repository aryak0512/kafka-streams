package com.aryak.kafka_stream.domain;

import lombok.Builder;

@Builder
public record Author(
        Integer id,
        String name,
        String country
) {
}
