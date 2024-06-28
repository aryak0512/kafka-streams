package com.aryak.kafka_stream.utils;

import java.util.function.Predicate;

public class Constants {

    private Constants() {
    }

    public static final String GREETINGS = "greetings";
    public static final String GREETINGS_UPPERCASE = "greetings_uppercase";
    public static final Predicate<String> greaterThan3 = s -> s.length() > 3;
    public static final String RESULT_TOPIC = "result";
}
