package com.aryak.kafka_stream.topology;

import com.aryak.kafka_stream.domain.*;
import com.aryak.kafka_stream.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static com.aryak.kafka_stream.utils.Constants.*;
import static org.apache.kafka.streams.kstream.Branched.as;

@Slf4j
public class TopologyFactory {

    private TopologyFactory() {

    }

    /**
     * Builds the topology, defined workflow of source processor, stream processor & sink processor
     *
     * @return
     */
    public static Topology buildTopology() {

        StreamsBuilder sb = new StreamsBuilder();

        // defined workflow of source processor, stream processor & sink processor
        sb.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> log.info("Consumed | Key:  {} | Value : {}", k, v))
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }

    /**
     * a topology to explore functions - filter & map
     *
     * @return topology
     */
    public static Topology buildTopology1() {

        StreamsBuilder sb = new StreamsBuilder();

        // defined workflow of source processor, stream processor & sink processor
        sb.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> greaterThan3.test(v))
                .mapValues((k, v) -> v.toUpperCase())
                .map((k, v) -> KeyValue.pair(k.toUpperCase(), new StringBuilder(v).reverse().toString()))
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }

    /**
     * a topology to explore functions - flatmap
     *
     * @return topology
     * @apiNote Input : null=ABC O/P : null-A , null-B, null-C
     */
    public static Topology buildTopology2() {

        StreamsBuilder sb = new StreamsBuilder();

        // defined workflow of source processor, stream processor & sink processor
        sb.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()))
                .flatMap((k, v) -> {
                    List<String> chars = Arrays.asList(v.split(""));
                    return chars.stream().map(c -> KeyValue.pair(k, c.toUpperCase())).toList();
                })
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }

    public static Topology buildTopology3() {
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String, String> inputChannel1 = sb.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> inputChannel2 = sb.stream(GREETINGS_UPPERCASE, Consumed.with(Serdes.String(), Serdes.String()));
        inputChannel2.merge(inputChannel1).to(RESULT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return sb.build();
    }

    /**
     * A topology that deals with custom product serde
     *
     * @return Topology
     */
    public static Topology buildTopology4() {

        StreamsBuilder sb = new StreamsBuilder();

        // defined workflow of source processor, stream processor & sink processor
        sb.stream(PRODUCTS, Consumed.with(Serdes.String(), SerdeFactory.productSerde()))
                .filter((k, v) -> greaterThan3.test(v.productName()))
                .peek((k,v) -> log.info("After peek | Key : {} | Value : {}" , k ,v))
                .mapValues((k, v) -> v.productName().toUpperCase())
                .peek((k,v) -> log.info("After map | Key : {} | Value : {}" , k ,v))
                .to(PRODUCTS_TRANSFORMED, Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }

    /**
     * A topology that deals with custom product serde
     *
     * @return Topology
     */
    public static Topology buildTopology5() {
        StreamsBuilder sb = new StreamsBuilder();
        sb.stream(PRODUCTS, Consumed.with(Serdes.String(), SerdeFactory.productSerdeUsingGeneric()))
                .filter((k, v) -> greaterThan3.test(v.productName()))
                .to(PRODUCTS_TRANSFORMED, Produced.with(Serdes.String(), SerdeFactory.productSerdeUsingGeneric()));
        return sb.build();
    }

    public static Topology buildKTable() {
        StreamsBuilder sb = new StreamsBuilder();
        sb.table(PRODUCTS,
                        Consumed.with(Serdes.String(), SerdeFactory.productSerdeUsingGeneric()),
                        Materialized.as("my-store")
                )
                .toStream()
                .peek((k, v) -> log.info("Key : {} | Value : {}", k, v));
        return sb.build();
    }


    /**
     * exploring the count stateful operation
     * @return topology
     */
    public static Topology buildTopology6() {

        StreamsBuilder sb = new StreamsBuilder();
        KStream<String, Product> productKStream = sb.stream(PRODUCTS, Consumed.with(Serdes.String(), SerdeFactory.productSerdeUsingGeneric()));

        // group by keys (keys should be non-null)
        KTable<String, Long> productKTable = productKStream.groupByKey()
                .count(Named.as("product-count"));

        // converting kTable to kStream and viewing content
        productKTable
                .toStream()
                .peek((k, v) -> log.info("Key : {} | Count : {}", k, v));

        return sb.build();
    }

    /**
     * exploring the reduce operation
     * <p>
     * After grouping by key, the reduce operator parameters provides access
     * to old value of that key (retrieved from internal kafka topic) and
     * the current value being computed
     * </p>
     *
     * @return the topology
     */
    public static Topology buildTopology7() {
        StreamsBuilder sb = new StreamsBuilder();
        var productKStream = sb.stream(PRODUCTS, Consumed.with(Serdes.String(), SerdeFactory.productSerdeUsingGeneric()));
        productKStream
                .groupByKey()
                .reduce((oldValue, newValue) -> {
                    log.info("Old product : {}", oldValue);
                    log.info("New product : {}", newValue);
                    return newValue;
                });
        return sb.build();
    }

    /**
     * exploring the aggregate operation
     * <p>
     * After grouping by key, the aggregate operator provides access
     * to old value of that key (retrieved from internal kafka topic) and
     * the current value being computed with the returned value permissible of any type
     * </p>
     *
     * @return the topology
     */
    public static Topology buildTopology8() {
        StreamsBuilder sb = new StreamsBuilder();
        var productKStream = sb.stream(PRODUCTS, Consumed.with(Serdes.String(), SerdeFactory.productSerdeUsingGeneric()));

        // step 1 - before using any aggregate functions
        KGroupedStream<String, Product> productKGroupedStream = productKStream.groupByKey();

        // step 2 - building arguments required for aggregate function
        Initializer<AggregateResult> aggregateResultInitializer = AggregateResult :: new;
        Aggregator<String, Product, AggregateResult > aggregator = (s, product, aggregateResult) -> null;

        productKGroupedStream.aggregate(aggregateResultInitializer,
                aggregator,
                Materialized.<String, AggregateResult, KeyValueStore<Bytes, byte[]>>as("ds")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdeFactory.aggregateResultSerde()))
                .toStream()
                .peek((k, v) -> log.info("Key : {} | Value : {}", k ,v));

        return sb.build();
    }

    /**
     * define the general order topology, and transforms payload in a revenue object
     * The revenue object can be published to a revenue topic later
     * @return the order topology
     */
    public static Topology buildTopology9() {
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String, Order> orderKStream = sb.stream(ORDERS, Consumed.with(Serdes.String(), SerdeFactory.orderSerdeUsingGeneric()));
        KeyValueMapper<String, Order, Revenue> revenueKeyValueMapper = (s, order) -> new Revenue(order.finalAmount(), order.locationId());
        orderKStream
                .mapValues((k, v) -> revenueKeyValueMapper.apply(k, v))
                .peek((k, v) -> log.info("Revenue : {} for key : {}", k ,v));
        return sb.build();
    }

    /**
     * define the general order topology, branch in restaurant and general orders
     * @return the order topology
     */
    public static Topology buildTopology10() {
        StreamsBuilder sb = new StreamsBuilder();
        Predicate<String, Order> generalOrderStrategy = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantOrderStrategy = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);
        KStream<String, Order> orderKStream = sb.stream(ORDERS, Consumed.with(Serdes.String(), SerdeFactory.orderSerdeUsingGeneric()));

        orderKStream
                .split(Named.as("orders-split"))
                .branch(generalOrderStrategy, Branched.withConsumer(generalOrderStream -> {

                    // transfer the filtered records to another topic
                    generalOrderStream.to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdeFactory.orderSerdeUsingGeneric()));
                }))
                .branch(restaurantOrderStrategy, Branched.withConsumer(restaurantOrderStream -> {

                    // transfer the filtered records to another topic
                    restaurantOrderStream.to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdeFactory.orderSerdeUsingGeneric()));
                }));

        return sb.build();
    }

    /**
     * get count per locationID in real time
     * @return the order topology
     */
    public static Topology buildTopology11() {
        StreamsBuilder sb = new StreamsBuilder();
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

        return sb.build();
    }

    /**
     * define the general order topology, and transforms payload in a revenue object
     * The revenue object can be published to a revenue topic later
     * @return the order topology
     */
    public static Topology buildTopology12() {
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String, Order> orderKStream = sb.stream(ORDERS, Consumed.with(Serdes.String(), SerdeFactory.orderSerdeUsingGeneric()));
        KGroupedStream<String, Order> stringOrderKGroupedStream = orderKStream.groupByKey();

        // step 1
        Initializer<AggregateRevenue> aggregateRevenueInitializer = AggregateRevenue::new;

        // step 2
        Aggregator<String, Order, AggregateRevenue> aggregator = (s, order, aggregateRevenue) -> aggregateRevenue.update(s, order);

        stringOrderKGroupedStream
                .aggregate(aggregateRevenueInitializer,
                        aggregator,
                        Materialized.<String, AggregateRevenue, KeyValueStore<Bytes, byte[]>>
                                as("my-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdeFactory.aggregateRevenueSerde())
                )
                .toStream()
                .peek((k,v) -> log.info("Key : {} | Value : {}", k, v));

        return sb.build();
    }
}
