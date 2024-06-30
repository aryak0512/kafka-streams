package com.aryak.kafka_stream.serdes;

import com.aryak.kafka_stream.domain.Product;
import org.apache.kafka.common.serialization.Serde;

public class SerdeFactory {

    private SerdeFactory() {
    }

    public static Serde<Product> productSerde(){
        return new ProductSerde();
    }
}
