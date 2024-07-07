package com.aryak.kafka_stream.domain;

import lombok.Builder;

import java.math.BigDecimal;
@Builder
public record Book(
        Long id,
        String title,
        BigDecimal price,
        Integer authorId
) {
}
