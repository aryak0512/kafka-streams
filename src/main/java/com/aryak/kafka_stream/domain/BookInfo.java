package com.aryak.kafka_stream.domain;

import java.math.BigDecimal;

public record BookInfo(
        Long id,
        String title,
        BigDecimal price,
        String name,
        String country

) {
}
