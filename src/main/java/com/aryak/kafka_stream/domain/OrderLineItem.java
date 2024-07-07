package com.aryak.kafka_stream.domain;

import lombok.Builder;

import java.math.BigDecimal;
@Builder
public record OrderLineItem(

        String item,
        Integer count,
        BigDecimal amount

) {


}
