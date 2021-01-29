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
package io.confluent.examples.streams.shoplifting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.sequences.SequenceState;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Arrays;

import static org.junit.Assert.*;

@Slf4j
public class GenericSequencesNegationByAggregationTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, SensorReading> inputTopic;
  private TestOutputTopic<String, SequenceState<SensorReading>> outputTopic;

  private final StringSerializer stringSerializer = new StringSerializer();
  private final StringDeserializer stringDeserializer = new StringDeserializer();

  @Before
  public void setup() {
    final StreamsBuilder builder = new StreamsBuilder();
    GenericSequencesNegationByAggregationExample.createTopology(builder);
    Topology topology = builder.build();
    System.err.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, TestUtils.streamsConfiguration());
    inputTopic = testDriver.createInputTopic(
            ShopLiftingJoinExample.SENSOR_READINGS,
            stringSerializer,
            new SensorReadingSerde().serializer()
    );
    outputTopic = testDriver.createOutputTopic(
            ShopLiftingJoinExample.SHOPLIFTS,
            stringDeserializer,
            new MyDeserializer()
    );
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
    inputTopic.pipeInput("1", new SensorReading("SHELF", 1000L), Instant.ofEpochMilli(1000L));
    assertTrue(outputTopic.isEmpty());
    inputTopic.pipeInput("1", new SensorReading("EXIT", 2000L), Instant.ofEpochMilli(2000L));
    assertFalse(outputTopic.isEmpty());
    final KeyValue<String, SequenceState<SensorReading>> keyValue = outputTopic.readKeyValue();
    assertEquals("1", keyValue.key);
    ObjectMapper objectMapper = new ObjectMapper();
    log.info(objectMapper.writeValueAsString(keyValue.value));
    assertEquals(
            Arrays.asList(
                    new SensorReading("SHELF", 1000L),
                    null,
                    new SensorReading("EXIT", 2000L)
            ),
            keyValue.value.getValues()
    );
    assertTrue(outputTopic.isEmpty());
  }

  @Test
  public void testAValidCheckout() {
    inputTopic.pipeInput("1", new SensorReading("SHELF", 1000L), Instant.ofEpochMilli(1000L));
    inputTopic.pipeInput("1", new SensorReading("SHELF", 1100L), Instant.ofEpochMilli(1100L));
    inputTopic.pipeInput("1", new SensorReading("COUNTER", 1500L), Instant.ofEpochMilli(1500L));
    inputTopic.pipeInput("1", new SensorReading("EXIT", 2000L), Instant.ofEpochMilli(2000L));
    inputTopic.pipeInput("1", new SensorReading("EXIT", 2100L) , Instant.ofEpochMilli(2100L));
    assertTrue(outputTopic.isEmpty());
  }

  @Test
  public void testDetectionOfParallelShopLifting() throws JsonProcessingException {
    inputTopic.pipeInput("1", new SensorReading("SHELF", 1000L), Instant.ofEpochMilli(1000L));
    inputTopic.pipeInput("2", new SensorReading("SHELF", 2000L), Instant.ofEpochMilli(2000L));
    inputTopic.pipeInput("3", new SensorReading("COUNTER", 3000L), Instant.ofEpochMilli(3000L));
    inputTopic.pipeInput("2", new SensorReading("EXIT", 3000L), Instant.ofEpochMilli(3000L));
    inputTopic.pipeInput("3", new SensorReading("COUNTER", 3000L), Instant.ofEpochMilli(3000L));
    inputTopic.pipeInput("1", new SensorReading("EXIT", 4000L), Instant.ofEpochMilli(4000L));
    KeyValue<String, SequenceState<SensorReading>> keyValue = outputTopic.readKeyValue();
    assertEquals("2", keyValue.key);
    ObjectMapper objectMapper = new ObjectMapper();
    log.info(objectMapper.writeValueAsString(keyValue.value));
    assertEquals(
            Arrays.asList(
                    new SensorReading("SHELF", 2000L),
                    null,
                    new SensorReading("EXIT", 3000L)
            ),
            keyValue.value.getValues()
    );
    keyValue = outputTopic.readKeyValue();
    assertEquals("1", keyValue.key);
    log.info(objectMapper.writeValueAsString(keyValue.value));
    assertEquals(
            Arrays.asList(
                    new SensorReading("SHELF", 1000L),
                    null,
                    new SensorReading("EXIT", 4000L)
            ),
            keyValue.value.getValues()
    );
    assertTrue(outputTopic.isEmpty());
  }

}

class MyDeserializer implements Deserializer<SequenceState<SensorReading>> {

  ObjectMapper objectMapper = new ObjectMapper();

  @SneakyThrows
  @Override
  public SequenceState<SensorReading> deserialize(String topic, byte[] data) {
    TypeReference<SequenceState<SensorReading>> sensorReadingListTypeReference =
            new TypeReference<SequenceState<SensorReading>>() {
              @Override
              public Type getType() {
                return super.getType();
              }
            };

    try {
      return objectMapper.readValue(data, sensorReadingListTypeReference);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

}