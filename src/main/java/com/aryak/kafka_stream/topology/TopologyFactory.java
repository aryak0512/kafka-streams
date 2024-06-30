package com.aryak.kafka_stream.topology;

import com.aryak.kafka_stream.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

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

        // defined workflow of source processor, stream processor & sink processor
        sb.stream(PRODUCTS, Consumed.with(Serdes.String(), SerdeFactory.productSerde()))
                .filter((k, v) -> greaterThan3.test(v.productName()))
                .to(PRODUCTS_TRANSFORMED, Produced.with(Serdes.String(), SerdeFactory.productSerde()));

        return sb.build();
    }
}
