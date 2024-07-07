package com.aryak.kafka_stream.domain;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
@Slf4j
public record AggregateRevenue(

        String locationId,
        int runningCount,
        BigDecimal amount
) {

    public AggregateRevenue() {
        this("", 0, BigDecimal.ZERO);
    }

    public AggregateRevenue update(String s, Order order) {
        var i = runningCount + 1;
        var amt = amount.add(order.finalAmount());
        return new AggregateRevenue(s, i, amt);
    }
}
