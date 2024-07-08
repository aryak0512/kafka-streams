package com.aryak.kafka_stream.utils;

import java.util.function.Predicate;

public class Constants {

    private Constants() {
    }

    public static final String GREETINGS = "greetings";
    public static final String GREETINGS_UPPERCASE = "greetings_uppercase";
    public static final Predicate<String> greaterThan3 = s -> s.length() > 3;
    public static final String RESULT_TOPIC = "result";
    public static final String PRODUCTS = "products";
    public static final String PRODUCTS_TRANSFORMED = "products_transformed";

    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";

    public static final String BOOKS = "books";
    public static final String AUTHORS = "authors";
    public static final String BOOK_INFO = "book_info";
}
