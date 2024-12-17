<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>io.confluent.developer</groupId>
    <artifactId>kafka-streams-example</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <java.version>11</java.version>
        <kafka.version>3.6.0</kafka.version> <!-- Update to your Kafka version -->
        <slf4j.version>2.0.9</slf4j.version>
    </properties>

    <dependencies>
        <!-- Kafka Streams -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- Kafka Clients (Producer and Consumer APIs) -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- JSON processing (for creating movie and rating JSON strings) -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.15.2</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>${slf4j.version}</version>
        </dependency>

        <!-- Kafka Streams Testing -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams-test-utils</artifactId>
            <version>${kafka.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- JUnit for Testing -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.10.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Compiler Plugin for Java Version -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>${java.version}</source>
                    <target>${java.version}</target>
                </configuration>
            </plugin>

            <!-- Shade Plugin to Package as a Fat JAR -->
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
                                    <mainClass>io.confluent.developer.KafkaInputProducer</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>


package io.confluent.developer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaInputProducer {

    private static final String MOVIE_INPUT_TOPIC = "movie-input";
    private static final String RATING_INPUT_TOPIC = "ratings-input";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092"); // Update with your Kafka broker address
        producerProps.put("key.serializer", LongSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("acks", "all");

        try (KafkaProducer<Long, String> producer = new KafkaProducer<>(producerProps)) {
            // Send movie data
            sendMovieData(producer);

            // Send rating data
            sendRatingData(producer);
        }
    }

    private static void sendMovieData(KafkaProducer<Long, String> producer) throws InterruptedException, ExecutionException {
        String[] movies = {
            "{\"id\": 1, \"title\": \"The Shawshank Redemption\", \"genre\": \"Drama\"}",
            "{\"id\": 2, \"title\": \"The Godfather\", \"genre\": \"Crime\"}",
            "{\"id\": 3, \"title\": \"The Dark Knight\", \"genre\": \"Action\"}"
        };

        for (int i = 0; i < movies.length; i++) {
            ProducerRecord<Long, String> record = new ProducerRecord<>(MOVIE_INPUT_TOPIC, (long) (i + 1), movies[i]);
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Movie sent: key=%d value=%s to partition=%d offset=%d%n", 
                              (long) (i + 1), movies[i], metadata.partition(), metadata.offset());
        }
    }

    private static void sendRatingData(KafkaProducer<Long, String> producer) throws InterruptedException, ExecutionException {
        String[] ratings = {
            "{\"id\": 1, \"userId\": 101, \"rating\": 4.8}",
            "{\"id\": 2, \"userId\": 102, \"rating\": 4.9}",
            "{\"id\": 3, \"userId\": 103, \"rating\": 4.7}"
        };

        for (int i = 0; i < ratings.length; i++) {
            ProducerRecord<Long, String> record = new ProducerRecord<>(RATING_INPUT_TOPIC, (long) (i + 1), ratings[i]);
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("Rating sent: key=%d value=%s to partition=%d offset=%d%n", 
                              (long) (i + 1), ratings[i], metadata.partition(), metadata.offset());
        }
    }
}