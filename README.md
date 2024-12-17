package io.confluent.developer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsToTable {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String STREAMS_OUTPUT_TOPIC = "streams-output-topic";
    public static final String TABLE_OUTPUT_TOPIC = "table-output-topic";

    // Method to build the Kafka Streams topology
    public Topology buildTopology(Properties properties) {
        final Serde<String> stringSerde = Serdes.String(); // Define Serde for String
        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream from the input topic
        final KStream<String, String> stream = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));

        // Convert the stream to a KTable and apply Serde to Materialized store
        final KTable<String, String> convertedTable = stream.toTable(
            Materialized.<String, String>as("stream-converted-to-table")
                .withKeySerde(stringSerde)
                .withValueSerde(stringSerde)
        );

        // Output the stream to a topic
        stream.to(STREAMS_OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        // Convert the table back to a stream and output to another topic
        convertedTable.toStream().to(TABLE_OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        // Return the constructed topology
        return builder.build(properties);
    }

    // Main method to set up and start the Kafka Streams application
    public static void main(String[] args) {
        Properties properties;
        if (args.length > 0) {
            properties = Utils.loadProperties(args[0]); // Load properties from file if provided
        } else {
            properties = Utils.loadProperties(); // Load default properties
        }
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-transforming");

        // Create an instance of the StreamsToTable class and build the topology
        StreamsToTable streamsToTable = new StreamsToTable();
        Topology topology = streamsToTable.buildTopology(properties);

        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
            // Set up a latch to keep the application running
            CountDownLatch countDownLatch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(5));
                countDownLatch.countDown();
            }));

            // For local running only; don't do this in production as it wipes out all local state
            kafkaStreams.cleanUp();
            kafkaStreams.start();

            // Wait for shutdown
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}