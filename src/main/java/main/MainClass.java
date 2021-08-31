package main;

import entity.JsonPojoDeserializer;
import entity.JsonPojoSerializer;
import entity.Produit;
import entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MainClass {
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();
    private static final Serde<Produit> PRODUIT_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(Produit.class));
    private static final Serde<User> USER_SERDE =
            Serdes.serdeFrom(new JsonPojoSerializer<>(), new JsonPojoDeserializer<>(User.class));

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-" + getRandomDoubleBetweenRange(1, 100));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        final StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, User> user = builder.stream("server_test_json.KafkaStreams.user",
                Consumed.with(STRING_SERDE, USER_SERDE)
        ).selectKey((key, value) -> {
            return (int) value.getAfter().get("id");
        });
        user.foreach((key, value) -> {
//            System.out.println("For user key: " + key + ", we have value user: " + value);
        });

        KStream<Integer, Produit> produit = builder.stream("server_test_json.KafkaStreams.produit",
                Consumed.with(STRING_SERDE, PRODUIT_SERDE)
        ).selectKey((key, value) -> {
            return (int) value.getAfter().get("id");
        });
        produit.foreach((key, value) -> {
//            System.out.println("For product key: " + key + ", we have value product: " + value);
        });

        KStream<Integer, Produit> produitKStream = produit.selectKey((key, value) -> {
            Integer valueOf = (Integer) value.getAfter().get("id_user");
            return valueOf != null ? valueOf : 0;
        });
        produitKStream.foreach((key, value) -> {
//            System.out.println("For product_user key: " + key + ", we have value product_user: " + value);
        });


        KStream<Integer, String> joined = produit.selectKey((key, value) -> {
                    Integer valueOf = (Integer) value.getAfter().get("id_user");
                    return valueOf != null ? valueOf : 0;
                }).
                join(user.selectKey((key, value) -> {
                            return (int) value.getAfter().get("id");
                        }),
                        (leftValue, rightValue) -> {
                            return "left=" + leftValue + ", right=" + rightValue;
                        },
                        JoinWindows.of(Duration.ofHours(20)),
                        StreamJoined.with(
                                INTEGER_SERDE, /* key */
                                PRODUIT_SERDE,   /* left value */
                                USER_SERDE) /* right value */
                );
        joined.foreach((key, value) -> {
//            System.out.println("Key = " + key + " and Value = " + value);
        });

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static double getRandomDoubleBetweenRange(double min, double max) {
        return (Math.random() * ((max - min) + 1)) + min;
    }
}
