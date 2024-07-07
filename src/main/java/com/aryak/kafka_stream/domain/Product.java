package com.aryak.kafka_stream.domain;

import lombok.Builder;

import java.time.LocalDateTime;

@Builder
public record Product(Integer productId,
                      String productName,
                      LocalDateTime createdAt) {

    @Override
    public String toString() {
        return
                "productName: " + productName +
                        " createdAt : " + createdAt;
    }
}



