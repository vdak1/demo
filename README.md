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