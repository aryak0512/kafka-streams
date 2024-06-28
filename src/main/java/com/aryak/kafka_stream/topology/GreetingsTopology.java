package com.aryak.kafka_stream.topology;

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
import static java.util.stream.Collectors.toList;

@Slf4j
public class GreetingsTopology {

    private GreetingsTopology(){

    }

    /**
     * Builds the topology, defined workflow of source processor, stream processor & sink processor
     * @return
     */
    public static Topology buildTopology(){

        StreamsBuilder sb = new StreamsBuilder();

        // defined workflow of source processor, stream processor & sink processor
        sb.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k,v) -> log.info("Consumed | Key:  {} | Value : {}", k , v))
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }

    /**
     * a topology to explore functions - filter & map
     * @return topology
     */
    public static Topology buildTopology1(){

        StreamsBuilder sb = new StreamsBuilder();

        // defined workflow of source processor, stream processor & sink processor
        sb.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k,v) -> greaterThan3.test(v))
                .mapValues((k,v) -> v.toUpperCase())
                .map((k,v) -> KeyValue.pair(k.toUpperCase(), new StringBuilder(v).reverse().toString()))
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }

    /**
     * a topology to explore functions - flatmap
     * @apiNote Input : null=ABC O/P : null-A , null-B, null-C
     * @return topology
     */
    public static Topology buildTopology2(){

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

    public static Topology buildTopology3(){
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String, String> inputChannel1 = sb.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> inputChannel2 = sb.stream(GREETINGS_UPPERCASE, Consumed.with(Serdes.String(), Serdes.String()));
        inputChannel2.merge(inputChannel1).to(RESULT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return sb.build();
    }
}
