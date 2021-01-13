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
package io.confluent.examples.streams;

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

import static org.junit.Assert.*;

/**
 * Stream processing unit test of {@link WordCountLambdaExample}, using TopologyTestDriver.
 *
 * See {@link WordCountLambdaExample} for further documentation.
 */
@Slf4j
public class SequenceExampleTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;

  private final StringSerializer stringSerializer = new StringSerializer();
  private final StringDeserializer stringDeserializer = new StringDeserializer();

  @Before
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    //Create Actual Stream Processing pipeline
    SequenceExample.createTopology(builder);
    testDriver = new TopologyTestDriver(builder.build(), WordCountLambdaExample.getStreamsConfiguration("localhost:9092"));
    inputTopic = testDriver.createInputTopic(SequenceExample.inputTopic,
                                             stringSerializer,
                                             stringSerializer);
    outputTopic = testDriver.createOutputTopic(SequenceExample.outputTopic,
                                               stringDeserializer,
                                               stringDeserializer);
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
    final KeyValue<String, String> keyValue = outputTopic.readKeyValue();
    assertEquals("1", keyValue.key);
    ObjectMapper objectMapper = new ObjectMapper();
    SensorData sensorData = objectMapper.readValue(keyValue.value, SensorData.class);
    log.info(objectMapper.writeValueAsString(sensorData));
    assertEquals(new SensorData(1000L, 0L, 2000L), sensorData);
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
