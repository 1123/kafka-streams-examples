package io.confluent.examples.streams.sorting.driver;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static io.confluent.examples.streams.sorting.stream.SortingOutOfOrderEventsExample.INPUT_TOPIC;

class MessageSender {

  private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

  private static KafkaProducer<Integer, String> messageProducer(final String bootstrapServers) {
    final Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return new KafkaProducer<>(producerProperties, Serdes.Integer().serializer(), Serdes.String().serializer());
  }

  void sendMessages(
          final String bootstrapServers,
          List<String> messages
  ) {
    log.info("Starting to send messages.");
    KafkaProducer<Integer, String> messageProducer = messageProducer(bootstrapServers);
    messages.forEach(m -> messageProducer.send(new ProducerRecord<>(INPUT_TOPIC, 3, m)));
    messageProducer.close();
    log.info("Finished sending messages.");
  }

}
