package com.aryak.kafka_stream.domain;

import lombok.Builder;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
@Builder
public record Order(

        Long orderId,
        String locationId,
        OrderType orderType,
        BigDecimal finalAmount,
        List<OrderLineItem> orderLineItems,
        LocalDateTime orderedDateTime
) {
}
