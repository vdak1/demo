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


<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.comp</groupId>
    <artifactId>customertransactionenrichment</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <description>Kafka Streams application for customer transaction enrichment</description>
    <properties>
        <java.version>11</java.version>
        <kafka.streams.version>3.5.1</kafka.streams.version>
        <slf4j.version>2.0.9</slf4j.version>
    </properties>

    <dependencies>
        <!-- Kafka Streams dependency -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>${kafka.streams.version}</version>
        </dependency>

        <!-- Kafka Clients for producers and consumers -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.streams.version}</version>
        </dependency>

        <!-- SLF4J for logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>${slf4j.version}</version>
        </dependency>

        <!-- Optional: Log4j2 for logging configuration -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>2.20.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-slf4j-impl</artifactId>
            <version>2.20.0</version>
        </dependency>

        <!-- JUnit for unit testing -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.0</version>
            <scope>test</scope>
        </dependency>

        <!-- Optional: Kafka Streams Testing Utilities -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams-test-utils</artifactId>
            <version>${kafka.streams.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Compiler Plugin -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>${java.version}</source>
                    <target>${java.version}</target>
                </configuration>
            </plugin>

            <!-- Shade Plugin to create an executable JAR -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <createDependencyReducedPom>false</createDependencyReducedPom>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.comp.events.CustomerTransactionEnrichment</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>

