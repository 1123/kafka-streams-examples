package io.confluent.examples.streams.sorting.stream;

import java.util.Objects;

public class Message implements Comparable<Message> {

  private long timestamp;

  public Message() {}

  public Message(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public int compareTo(Message o) {
    return Long.compare(timestamp, o.timestamp);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Message)) return false;
    Message message = (Message) o;
    return timestamp == message.timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @SuppressWarnings("unused")
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp);
  }
}
