package com.aryak.kafka_stream.topology;

import com.aryak.kafka_stream.domain.AggregateResult;
import com.aryak.kafka_stream.domain.Product;
import com.aryak.kafka_stream.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;

import static com.aryak.kafka_stream.utils.Constants.*;

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
        //Initializer<AggregateResult> aggregateResultInitializer = AggregateResult :: new;
        Aggregator<String, Product, AggregateResult > aggregator = new Aggregator<String, Product, AggregateResult>() {
            @Override
            public AggregateResult apply(String s, Product product, AggregateResult aggregateResult) {
                return null;
            }
        };

        //productKGroupedStream.aggregate()
        return sb.build();
    }
}
