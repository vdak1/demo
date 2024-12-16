import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Properties;

public class FlinkKafkaJoin {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");

        // Bank Accounts KTable source
        FlinkKafkaConsumer<String> bankAccountsConsumer = new FlinkKafkaConsumer<>(
                "bank_accounts",
                new SimpleStringSchema(),
                properties
        );
        DataStream<String> bankAccountsStream = env
                .addSource(bankAccountsConsumer)
                .name("Bank Accounts Stream");

        // Transactions Stream source
        FlinkKafkaConsumer<String> transactionsConsumer = new FlinkKafkaConsumer<>(
                "transactions",
                new SimpleStringSchema(),
                properties
        );
        DataStream<String> transactionsStream = env
                .addSource(transactionsConsumer)
                .name("Transactions Stream");

        // Convert streams to tables
        tableEnv.executeSql(
                "CREATE TABLE BankAccounts (" +
                        "account_id STRING, " +
                        "customer_name STRING, " +
                        "balance DOUBLE, " +
                        "PRIMARY KEY (account_id) NOT ENFORCED" +
                        ") WITH (" +
                        "'connector' = 'kafka', " +
                        "'topic' = 'bank_accounts', " +
                        "'properties.bootstrap.servers' = 'localhost:9092', " +
                        "'format' = 'json', " +
                        "'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE Transactions (" +
                        "transaction_id STRING, " +
                        "account_id STRING, " +
                        "amount DOUBLE, " +
                        "timestamp BIGINT, " +
                        "WATERMARK FOR timestamp AS TIMESTAMP_MILLIS(timestamp) - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "'connector' = 'kafka', " +
                        "'topic' = 'transactions', " +
                        "'properties.bootstrap.servers' = 'localhost:9092', " +
                        "'format' = 'json', " +
                        "'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        // Perform the join
        Table joinedTable = tableEnv.sqlQuery(
                "SELECT " +
                        "t.transaction_id, " +
                        "t.account_id, " +
                        "b.customer_name, " +
                        "b.balance, " +
                        "t.amount, " +
                        "t.timestamp " +
                        "FROM Transactions AS t " +
                        "JOIN BankAccounts FOR SYSTEM_TIME AS OF t.timestamp AS b " +
                        "ON t.account_id = b.account_id"
        );

        // Convert the result table back to a data stream
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toChangelogStream(joinedTable);

        // Output the joined results
        resultStream.print();

        // Execute the Flink job
        env.execute("Flink Kafka KTable-Stream Join");
    }
}