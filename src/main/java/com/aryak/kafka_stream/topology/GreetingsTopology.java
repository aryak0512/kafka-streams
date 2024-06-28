package com.aryak.kafka_stream.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import static com.aryak.kafka_stream.utils.Constants.GREETINGS;
import static com.aryak.kafka_stream.utils.Constants.GREETINGS_UPPERCASE;

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
                .mapValues((k,v) -> v.toUpperCase())
                .peek((k,v) -> log.info("Produced | Key:  {} | Value : {}", k , v))
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }
}
