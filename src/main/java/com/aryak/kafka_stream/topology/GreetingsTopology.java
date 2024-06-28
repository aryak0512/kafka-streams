package com.aryak.kafka_stream.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingsTopology {

    private GreetingsTopology(){

    }

    public static Topology buildTopology(){

        StreamsBuilder sb = new StreamsBuilder();

        sb.stream("greetings", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((k,v) -> v.toUpperCase())
                .to("greetings_uppercase", Produced.with(Serdes.String(), Serdes.String()));

        return sb.build();
    }
}
