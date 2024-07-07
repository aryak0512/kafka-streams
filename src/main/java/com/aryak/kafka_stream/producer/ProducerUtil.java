package com.aryak.kafka_stream.producer;

import com.aryak.kafka_stream.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;


/**
 * @author aryak
 * A utility bean to produce test data into kafka topic
 */

@Slf4j
public class ProducerUtil {

    private ProducerUtil() {
    }

    /**
     * configure the producer properties
     *
     * @return the producer properties
     */
    public static Properties getProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }


    /**
     * Populate dummy product list to push into kafka topics
     *
     * @return List of product
     */
    public static List<Product> getProducts() {

        var p1 = Product.builder()
                .createdAt(LocalDateTime.now())
                .productId(11213)
                .productName("Product 1")
                .build();

        var p2 = Product.builder()
                .createdAt(LocalDateTime.now())
                .productId(535353)
                .productName("Product 2")
                .build();

        var p3 = Product.builder()
                .createdAt(LocalDateTime.now())
                .productId(657577)
                .productName("Product 3")
                .build();

        return List.of(p1, p2, p3);
    }

    public static List<Order> getOrders() {

        var order1 = Order.builder()
                .orderId(12345L)
                .finalAmount(BigDecimal.valueOf(27.00))
                .orderedDateTime(LocalDateTime.now())
                .locationId("store_5678")
                .orderType(OrderType.GENERAL)
                .orderLineItems(List.of(

                        OrderLineItem.builder()
                                .item("Bananas")
                                .count(2)
                                .amount(BigDecimal.valueOf(2.00))
                                .build(),

                        OrderLineItem.builder()
                                .item("Iphone charger")
                                .count(1)
                                .amount(BigDecimal.valueOf(25.00))
                                .build()

                ))
                .build();

        var order2 = Order.builder()
                .orderId(6356732L)
                .finalAmount(BigDecimal.valueOf(27.00))
                .orderedDateTime(LocalDateTime.now())
                .locationId("store_1234")
                .orderType(OrderType.RESTAURANT)
                .orderLineItems(List.of(

                        OrderLineItem.builder()
                                .item("Nachos")
                                .count(2)
                                .amount(BigDecimal.valueOf(2.00))
                                .build(),

                        OrderLineItem.builder()
                                .item("Pasta")
                                .count(1)
                                .amount(BigDecimal.valueOf(25.00))
                                .build()

                ))
                .build();

        var order3 = Order.builder()
                .orderId(235276L)
                .finalAmount(BigDecimal.valueOf(27.00))
                .orderedDateTime(LocalDateTime.now())
                .locationId("store_5678")
                .orderType(OrderType.GENERAL)
                .orderLineItems(List.of(

                        OrderLineItem.builder()
                                .item("Bananas")
                                .count(2)
                                .amount(BigDecimal.valueOf(2.00))
                                .build(),

                        OrderLineItem.builder()
                                .item("Iphone charger")
                                .count(1)
                                .amount(BigDecimal.valueOf(25.00))
                                .build()

                ))
                .build();

        var order4 = Order.builder()
                .orderId(89763L)
                .finalAmount(BigDecimal.valueOf(27.00))
                .orderedDateTime(LocalDateTime.now())
                .locationId("store_1234")
                .orderType(OrderType.RESTAURANT)
                .orderLineItems(List.of(

                        OrderLineItem.builder()
                                .item("Fish and chips")
                                .count(2)
                                .amount(BigDecimal.valueOf(2.00))
                                .build(),

                        OrderLineItem.builder()
                                .item("Burger")
                                .count(1)
                                .amount(BigDecimal.valueOf(25.00))
                                .build()

                ))
                .build();

        return List.of(order1, order2, order3, order4);
    }

    public static List<Book> getBooks() {

        Book book1 = Book.builder()
                .id(100L)
                .price(BigDecimal.valueOf(5.00))
                .title("Head first Design patterns")
                .authorId(1)
                .build();

        Book book2 = Book.builder()
                .id(101L)
                .price(BigDecimal.valueOf(15.00))
                .title("Concurrency in Java")
                .authorId(2)
                .build();

        return List.of(book1, book2);
    }

    public static List<Author> getAuthors() {
        var author1 = Author.builder().name("Kathy Sierra").country("USA").id(1).build();
        var author2 = Author.builder().name("Doug Lea").country("USA").id(2).build();
        return List.of(author1, author2);
    }
}
