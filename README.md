package com.comp.events;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * The main class responsible for setting up and running the Kafka Streams application
 * to enrich customer transactions from the 'transactions' topic using data from the
 * 'customer-accounts' topic. Enriched data is sent to the 'enriched-transactions' topic.
 */
public class CustomerTransactionEnrichment {

    private static final String APPLICATION_ID = "customer-transaction-enrichment";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String STATE_STORE_ENV = "STATE_STORE_PATH";
    private static final String DEFAULT_STATE_DIR = "/tmp/kafka-streams/state-store";

    private static final String CUSTOMER_ACCOUNTS_TOPIC = "customer-accounts";
    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String ENRICHED_TRANSACTIONS_TOPIC = "enriched-transactions";
    private static final String DLQ_TOPIC = "dead-letter-queue-topic";

/**
 * The application entry point, responsible for configuring the Kafka Streams application
 * and initializing the processing pipeline.
 *
 * @param args command-line arguments (if any) for the application
 */
public static void main(String[] args) {

Properties streamsProperties = configureStreamProperties();
        KafkaProducer<String, String> deadLetterQueueProducer = configureKafkaProducer();
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> customerAccounts = builder.table(CUSTOMER_ACCOUNTS_TOPIC);
        KStream<String, String> transactions = buildTransactionStream(builder);

        processEnrichedTransactions(transactions, customerAccounts, deadLetterQueueProducer);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);
        setupShutdownHooks(streams);
        streams.start();
    }

/**
 * Configures and returns the stream processing properties for the Kafka Streams application,
 * including application ID, bootstrap servers, key/value serializers, and state store directory.
 *
 * @return Properties object containing Kafka Streams configuration
 */
private static Properties configureStreamProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, System.getenv(STATE_STORE_ENV) != null
                ? System.getenv(STATE_STORE_ENV)
                : DEFAULT_STATE_DIR);
        return properties;
    }

/**
 * Configures and returns a KafkaProducer to send error messages to the Dead Letter Queue (DLQ).
 *
 * @return KafkaProducer for sending messages to the DLQ topic
 */
private static KafkaProducer<String, String> configureKafkaProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(producerProperties);
    }

/**
 * Builds and returns a KStream for transactions consumed from the 'transactions' topic.
 * Filters out invalid transactions and logs any invalid data detected.
 *
 * @param builder StreamsBuilder to construct the KStream
 * @return KStream for valid transactions from the topic
 */
private static KStream<String, String> buildTransactionStream(StreamsBuilder builder) {
        return builder.stream(
                        TRANSACTIONS_TOPIC,
                        Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> key != null && value != null && !value.isEmpty())
                .peek((key, value) -> {
                    if (key == null || value.isEmpty()) {
                        System.out.println("Invalid transaction detected: Key = " + key + ", Value = " + value);
                    }
                });
    }

/**
 * Joins the transactions stream with the customer accounts table to enrich the transactions.
 * Enriched transactions are sent to the 'enriched-transactions' topic, and any failed transactions
 * are logged and sent to the Dead Letter Queue (DLQ) topic.
 *
 * @param transactions           KStream containing the transactions
 * @param customerAccounts       KTable containing customer account data
 * @param deadLetterQueueProducer KafkaProducer for sending error messages to the DLQ
 */
private static void processEnrichedTransactions(KStream<String, String> transactions,
                                                    KTable<String, String> customerAccounts,
                                                    KafkaProducer<String, String> deadLetterQueueProducer) {
        KStream<String, String> enrichedTransactions = transactions
                .join(customerAccounts, (transactionValue, customerAccountValue) ->
                        transactionValue + "," + customerAccountValue);

        enrichedTransactions.foreach((key, value) -> {
            if (value == null) {
                System.out.println("Failed to enrich transaction for key: " + key);
                deadLetterQueueProducer.send(new ProducerRecord<>(DLQ_TOPIC, key, "Enrichment failed"));
            }
        });

        enrichedTransactions.to(ENRICHED_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

/**
 * Registers shutdown hooks to handle graceful termination of the Kafka Streams application,
 * and to log any uncaught exceptions during streaming operations.
 *
 * @param streams KafkaStreams instance to which hooks will be added
 */
private static void setupShutdownHooks(KafkaStreams streams) {
        streams.setUncaughtExceptionHandler((thread, exception) -> {
            System.err.println("Stream encountered an uncaught exception: " + exception.getMessage());
            streams.close();
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Kafka Streams application...");
            streams.close();
        }));
    }
}


