package io.confluent.examples.streams.sorting;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.sorting.driver.SortingOutOfORderEventsExampleDriver;
import io.confluent.examples.streams.sorting.stream.SortingOutOfOrderEventsExample;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class SortingOutOfOrderEventsIntegrationTest {

  @ClassRule
  public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

  @BeforeClass
  public static void startKafkaCluster() {
    CLUSTER.createTopic(SortingOutOfOrderEventsExample.INPUT_TOPIC);
    CLUSTER.createTopic(SortingOutOfOrderEventsExample.OUTPUT_TOPIC);
  }

  @Test
  public void runTest() throws InterruptedException {
    SortingOutOfOrderEventsExample.main(new String[0]);
    Thread.sleep(1000);
    SortingOutOfORderEventsExampleDriver.main(new String[0]);
    Thread.sleep(1000);
  }

}
