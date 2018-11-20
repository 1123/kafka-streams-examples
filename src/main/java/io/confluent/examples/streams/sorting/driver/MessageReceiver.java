package io.confluent.examples.streams.sorting.driver;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

import static io.confluent.examples.streams.sorting.stream.SortingOutOfOrderEventsExample.OUTPUT_TOPIC;

class MessageReceiver {

  private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

  private Properties consumerProperties(final String bootstrapServers) {
    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "global-tables-consumer");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return consumerProps;
  }

  void receiveSortedMessages(
          final String bootstrapServers,
          final int expected
  ) {
    final KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers));
    consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
    int received = 0;
    while(received < expected) {
      final ConsumerRecords<Long, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
      received += records.count();
      log.info("Received {} values", received);
    }
    consumer.close();
  }

}
