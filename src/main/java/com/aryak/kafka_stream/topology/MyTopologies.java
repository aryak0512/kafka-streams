package com.aryak.kafka_stream.topology;

import com.aryak.kafka_stream.domain.Order;
import com.aryak.kafka_stream.domain.OrderType;
import com.aryak.kafka_stream.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.aryak.kafka_stream.utils.Constants.*;
import static com.aryak.kafka_stream.utils.Constants.RESTAURANT_ORDERS_COUNT;

@Slf4j
@Component
public class MyTopologies {

    /**
     * get count per locationID in real time
     * @return the order topology
     */
    @Autowired
    public void buildTopology11(StreamsBuilder sb ) {

        Predicate<String, Order> generalOrderStrategy = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantOrderStrategy = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);
        KStream<String, Order> orderKStream = sb.stream(ORDERS, Consumed.with(Serdes.String(), SerdeFactory.orderSerdeUsingGeneric()));

        // get count per locationID in real time
        orderKStream
                .split(Named.as("orders-split"))
                .branch(generalOrderStrategy, Branched.withConsumer(generalOrderStream -> {

                    KTable<String, Long> generalCountKTable = generalOrderStream
                            .groupByKey()
                            .count(Named.as(GENERAL_ORDERS_COUNT), Materialized.as(GENERAL_ORDERS_COUNT));

                    generalCountKTable.toStream().peek((k, v) -> log.info("General | K : {} | V : {}", k, v));
                }))
                .branch(restaurantOrderStrategy, Branched.withConsumer(restaurantOrderStream -> {

                    KTable<String, Long> generalCountKTable = restaurantOrderStream
                            .groupByKey()
                            .count(Named.as(RESTAURANT_ORDERS_COUNT), Materialized.as(RESTAURANT_ORDERS_COUNT));

                    generalCountKTable.toStream().peek((k, v) -> log.info("Restaurant | K : {} | V : {}", k, v));
                }));
    }
}
