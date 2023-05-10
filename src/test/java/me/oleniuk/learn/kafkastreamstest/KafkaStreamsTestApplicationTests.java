package me.oleniuk.learn.kafkastreamstest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest
@EnableScheduling
class KafkaStreamsTestApplicationTests {

    private final AtomicInteger indexer = new AtomicInteger(0);
    private final static String TEST_TOPIC_1 = "test_topic_1";

    private final static String TEST_TOPIC_2 = "test_topic_2";

    @Container
    private KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    @BeforeEach
    public void initKafka() {
        createTopics(TEST_TOPIC_1, TEST_TOPIC_2);
    }

    @Test
    void test_send_receive() throws InterruptedException {
//        startSimpleConsumer();
        publishUnorderedMessagesToTopics();
        startKafkaStreamsConsumer();
    }

    private void startKafkaStreamsConsumer() throws InterruptedException {
        var sortedList = new ArrayList<Integer>();
        for (int i = 0; i <= 100; i++) {
            sortedList.add(i);
            sortedList.add(i);
        }
        var values  = new ArrayList<>();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        KStream<Person, String> stream1 = streamsBuilder.stream(TEST_TOPIC_1, Consumed.with(new PersonSerde(), stringSerde));
        KStream<String, String> stream2 = streamsBuilder.stream(TEST_TOPIC_2, Consumed.with(stringSerde, stringSerde));

        KStream<String, String> mergedStream = stream1.selectKey((k, v) -> k.anonymize())
                .merge(stream2)
                .peek((key, value) -> System.out.printf("Stream Consumer Record:(%s, %s)\n", key, value));

        mergedStream.process(TimestampOrderingProcessor::new);
        mergedStream.foreach((k, v) -> values.add(Long.valueOf(v)));

        KafkaStreams myStream = new KafkaStreams(streamsBuilder.build(), getStreamsConfiguration(getKafkaBrokers()));
        myStream.cleanUp();
        myStream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(myStream::close));

        System.out.println("Kafka Streams Started");

        Thread.sleep(10_000);

        assertEquals(sortedList, values);

        myStream.close();
    }

    static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "integration-test-streams");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "integration-test-streams-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return streamsConfiguration;
    }


    private void publish10MessagesToTopic(String topic, int init) {
        Producer<String, String> producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers(), ProducerConfig.CLIENT_ID_CONFIG, "integration_test_producer", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(), ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

        for (int i = init; i < init + 10; i++)
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
        System.out.println("Message sent successfully");
        producer.close();
    }

    private void publishUnorderedMessagesToTopics() {
        Producer<String, String> producer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers(),
                        ProducerConfig.CLIENT_ID_CONFIG, "integration_test_producer",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
                ));
        var random = new Random();
        for (int i = 0; i <= 100; i++) {
            var timeStampOfBefore = System.currentTimeMillis() - random.nextInt(1000000);
            var timeStampOfAfter = System.currentTimeMillis() + random.nextInt(1000000);
            producer.send( new ProducerRecord<>(
                    /*(random.nextInt(5000) % 2 == 0) ? TEST_TOPIC_1 : TEST_TOPIC_2*/
                    TEST_TOPIC_2,
                    Integer.toString(i),
                    Integer.toString(i)
            ));

            producer.send( new ProducerRecord<>(
                    /*(random.nextInt(5000) % 2 == 0) ? TEST_TOPIC_1 : TEST_TOPIC_2*/
                    TEST_TOPIC_1,
                    Integer.toString(i),
                    Integer.toString(i)
            ));

//            record.headers().add(new RecordHeader("timestamp_type", "CreateTime".getBytes()));
//            record.headers().add(new RecordHeader("timestamp", ByteBuffer.allocate(8).putLong(
//                    (random.nextInt(5000) % 2 == 0) ? timeStampOfBefore : timeStampOfAfter
//            ).array()));

        }
        System.out.println("Message sent successfully");
        //producer.close();
    }

    private void startSimpleConsumer() {
        Consumer<String, String> consumer = new KafkaConsumer<>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers(), ConsumerConfig.GROUP_ID_CONFIG, "integration_test_producer_group", ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(), ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        consumer.subscribe(Set.of(TEST_TOPIC_1));

        int consumed = 0;
        ConsumerRecords<String, String> consumerRecords;
        do {
            consumerRecords = consumer.poll(Duration.ofSeconds(1));
            consumed += consumerRecords.count();
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(), record.partition(), record.offset());
            });
            consumer.commitAsync();
        } while (!consumerRecords.isEmpty());

        consumer.close();
        System.out.println("DONE");

        assertEquals(10, consumed);
    }


    private void createTopics(String... topics) {
        var newTopics = Arrays.stream(topics).map(topic -> new NewTopic(topic, 1, (short) 1)).collect(Collectors.toList());
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokers()))) {
            admin.createTopics(newTopics);
        }
    }

    private String getKafkaBrokers() {
        Integer mappedPort = kafkaContainer.getFirstMappedPort();
        return String.format("%s:%d", "localhost", mappedPort);
    }

    //@Scheduled(fixedDelay = 5000L)
    public void publish() {
        publish10MessagesToTopic(TEST_TOPIC_1, indexer.addAndGet(10));
        publish10MessagesToTopic(TEST_TOPIC_2, indexer.get());
    }

}
