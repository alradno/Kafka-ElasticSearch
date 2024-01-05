package com.example.kafka_es;

import org.apache.http.HttpHost;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class Kafka_Elastic_Main {

    static final Properties PROPS = new Properties();
    public static void main(String[] args) throws IOException {

        // Load properties from file located in args[0]
        try (InputStream input = new FileInputStream(args[0])) {
            PROPS.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //Configuracion de Kafka Streams
        KafkaStreamsConfig configClass = new KafkaStreamsConfig();
        Properties config = configClass.setProperties(PROPS.getProperty("application.id"), PROPS.getProperty("bootstrap.servers"));

        //Configuracion Topology
        String source_topic = PROPS.getProperty("source.topic");
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        String indexName = PROPS.getProperty("elasticSearch.index.name");

        Topology topology = configClass.setTopology(source_topic, client, indexName);

        try (KafkaStreams streams = new KafkaStreams(topology, config)) {

            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
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
        }
        System.exit(0);

    }

}
