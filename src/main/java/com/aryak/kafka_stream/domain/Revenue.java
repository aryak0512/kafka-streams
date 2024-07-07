package com.aryak.kafka_stream.domain;

import java.math.BigDecimal;

public record Revenue(

        BigDecimal finalAmount,
        String locationId
) {

}
