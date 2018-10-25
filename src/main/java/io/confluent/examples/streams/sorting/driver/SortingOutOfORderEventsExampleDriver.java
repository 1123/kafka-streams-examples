package io.confluent.examples.streams.sorting.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SortingOutOfORderEventsExampleDriver {
  private static final int RECORDS_TO_GENERATE = 100;

  private static final Logger log = LoggerFactory.getLogger(MessageSender.class);

  public static void main(String[] args) {
    log.info("Starting up.");
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    log.info("Calling generate messages.");
    List<String> messages = new MessageGenerator().generateOutOfOrderMessages(RECORDS_TO_GENERATE * 2);
    log.info("Calling send messages.");
    new MessageSender().sendMessages(bootstrapServers, messages);
    log.info("Calling receive messages.");
    new MessageReceiver().receiveSortedMessages(bootstrapServers, RECORDS_TO_GENERATE);
  }

}

