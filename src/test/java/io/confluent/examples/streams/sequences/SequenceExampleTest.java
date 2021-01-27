/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams.sequences;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Stream processing unit test of {@link SequenceExample}, using TopologyTestDriver.
 *
 * See {@link SequenceExample} for further documentation.
 */
@Slf4j
public class SequenceExampleTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, SequenceState<String>> outputTopic;

  private final StringSerializer stringSerializer = new StringSerializer();
  private final StringDeserializer stringDeserializer = new StringDeserializer();

  @Before
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    SequenceExample.createTopology(builder);
    testDriver = new TopologyTestDriver(builder.build(), SequenceExample.getStreamsConfiguration());
    inputTopic = testDriver.createInputTopic(SequenceExample.SENSOR_READINGS_TOPIC,
                                             stringSerializer,
                                             stringSerializer);
    outputTopic = testDriver.createOutputTopic(SequenceExample.SHOPLIFTS_TOPIC,
                                               stringDeserializer,
                                               new MySequenceStateSerde<String>().deserializer());
  }

  @After
  public void tearDown() {
    try {
      testDriver.close();
    } catch (final RuntimeException e) {
      // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
      // Logged stacktrace cannot be avoided
      System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
    }
  }

  /**
   *  Test detection of shop lifting.
   */
  @Test
  public void testThatTheTopologyDetectsShopLifting() throws JsonProcessingException {
    inputTopic.pipeInput("1", "SHELF", Instant.ofEpochMilli(1000L));
    assertTrue(outputTopic.isEmpty());
    inputTopic.pipeInput("1", "EXIT", Instant.ofEpochMilli(2000L));
    assertFalse(outputTopic.isEmpty());
    final KeyValue<String, SequenceState<String>> keyValue = outputTopic.readKeyValue();
    assertEquals("1", keyValue.key);
    ObjectMapper objectMapper = new ObjectMapper();
    SequenceState<String> sequenceState = keyValue.value;
    log.info(objectMapper.writeValueAsString(sequenceState));
    assertEquals(
            new SequenceState<>(
                    3,
                    2000L,
                    Arrays.asList("SHELF", null, "EXIT")
            ), sequenceState
    );
  }

  /**
   *  Test detection of shop lifting.
   */
  @Test
  public void testDetectionOfParallelShopLifting() throws JsonProcessingException {
    inputTopic.pipeInput("1", "SHELF", Instant.ofEpochMilli(1000L));
    inputTopic.pipeInput("2", "SHELF", Instant.ofEpochMilli(2000L));
    inputTopic.pipeInput("3", "COUNTER", Instant.ofEpochMilli(3000L));
    inputTopic.pipeInput("2", "EXIT", Instant.ofEpochMilli(3000L));
    inputTopic.pipeInput("3", "COUNTER", Instant.ofEpochMilli(3000L));
    inputTopic.pipeInput("1", "EXIT", Instant.ofEpochMilli(4000L));
    KeyValue<String, SequenceState<String>> keyValue = outputTopic.readKeyValue();
    assertEquals("2", keyValue.key);
    ObjectMapper objectMapper = new ObjectMapper();
    log.info(objectMapper.writeValueAsString(keyValue.value));
    assertEquals(
            new SequenceState<>(
                    3,
                    3000L,
                    Arrays.asList("SHELF", null, "EXIT")
            ), keyValue.value
    );
    keyValue = outputTopic.readKeyValue();
    assertEquals("1", keyValue.key);
    log.info(objectMapper.writeValueAsString(keyValue.value));
    assertEquals(
            new SequenceState<>(
                    3,
                    4000L,
                    Arrays.asList("SHELF", null, "EXIT")
            ), keyValue.value
    );
    assertTrue(outputTopic.isEmpty());
  }


  /**
   *  Test a valid checkout. No shop lifting should be detected.
   */
  @Test
  public void testAValidCheckout() {
    inputTopic.pipeInput("1", "SHELF", Instant.ofEpochMilli(1000L));
    inputTopic.pipeInput("1", "SHELF", Instant.ofEpochMilli(1100L));
    inputTopic.pipeInput("1", "COUNTER", Instant.ofEpochMilli(1500L));
    inputTopic.pipeInput("1", "EXIT", Instant.ofEpochMilli(2000L));
    inputTopic.pipeInput("1", "EXIT", Instant.ofEpochMilli(2100L));
    assertTrue(outputTopic.isEmpty());
  }

}
