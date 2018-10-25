package io.confluent.examples.streams.sorting.stream;

import java.util.SortedSet;
import java.util.TreeSet;

class MessageStore {

  private TreeSet<Message> messages = new TreeSet<>();

  public SortedSet<Message> getMessages() {
    return messages;
  }
}
