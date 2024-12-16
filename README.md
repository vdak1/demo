import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class KafkaEventProducer {
    private static final String BANK_ACCOUNTS_TOPIC = "bank_accounts";
    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Kafka producer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Initialize Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            // Produce events to bank_accounts topic
            for (int i = 1; i <= 1_000_000; i++) {
                String accountId = String.format("A%06d", i); // e.g., A000001, A000002
                String customerName = "Customer" + i;
                double balance = new Random().nextDouble() * 10000; // Random balance

                String accountEvent = String.format("{\"account_id\": \"%s\", \"customer_name\": \"%s\", \"balance\": %.2f}",
                        accountId, customerName, balance);

                sendEvent(producer, BANK_ACCOUNTS_TOPIC, accountId, accountEvent);
            }

            // Produce events to transactions topic
            for (int i = 1; i <= 16_000_000; i++) { // 16 transactions per 1 million accounts
                String transactionId = "T" + i;
                String accountId = String.format("A%06d", new Random().nextInt(1_000_000) + 1); // Random account ID
                double amount = (new Random().nextDouble() - 0.5) * 200; // Random transaction (-100 to +100)
                long timestamp = System.currentTimeMillis();

                String transactionEvent = String.format("{\"transaction_id\": \"%s\", \"account_id\": \"%s\", \"amount\": %.2f, \"timestamp\": %d}",
                        transactionId, accountId, amount, timestamp);

                sendEvent(producer, TRANSACTIONS_TOPIC, transactionId, transactionEvent);

                // Sleep to simulate real-time events
                if (i % 1000 == 0) {
                    Thread.sleep(10);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void sendEvent(KafkaProducer<String, String> producer, String topic, String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        RecordMetadata metadata = producer.send(record).get();
        System.out.printf("Sent event to topic %s: key=%s, value=%s, partition=%d, offset=%d%n",
                topic, key, value, metadata.partition(), metadata.offset());
    }
}