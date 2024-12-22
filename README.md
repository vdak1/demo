package io.confluent.developer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.developer.avro.Movie;
import io.confluent.developer.avro.Rating;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MovieRatingProducer {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        final Producer<String, Object> producer = new KafkaProducer<>(props);

        // Produce movie data
        produceMovie(producer, 294, "Die Hard", 1988);
        produceMovie(producer, 354, "Tree of Life", 2011);
        produceMovie(producer, 782, "A Walk in the Clouds", 1998);
        produceMovie(producer, 128, "The Big Lebowski", 1998);
        produceMovie(producer, 780, "Super Mario Bros.", 1993);

        // Produce rating data
        produceRating(producer, 294, 8.2);
        produceRating(producer, 294, 8.5);
        produceRating(producer, 354, 9.9);
        produceRating(producer, 354, 9.7);
        produceRating(producer, 782, 7.8);
        produceRating(producer, 782, 7.7);
        produceRating(producer, 128, 8.7);
        produceRating(producer, 128, 8.4);
        produceRating(producer, 780, 2.1);

        producer.flush();
        producer.close();
    }

    private static void produceMovie(Producer<String, Object> producer, int id, String title, int releaseYear) {
        Movie movie = Movie.newBuilder()
                .setId(id)
                .setTitle(title)
                .setReleaseYear(releaseYear)
                .build();

        ProducerRecord<String, Object> record = new ProducerRecord<>("movies", String.valueOf(id), movie);
        try {
            producer.send(record).get();
            System.out.println("Produced movie: " + movie);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void produceRating(Producer<String, Object> producer, int id, double rating) {
        Rating ratingObj = Rating.newBuilder()
                .setId(id)
                .setRating(rating)
                .build();

        ProducerRecord<String, Object> record = new ProducerRecord<>("ratings", String.valueOf(id), ratingObj);
        try {
            producer.send(record).get();
            System.out.println("Produced rating: " + ratingObj);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
