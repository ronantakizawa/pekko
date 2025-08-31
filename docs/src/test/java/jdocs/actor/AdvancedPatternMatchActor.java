/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package jdocs.actor;

// #imports
import java.util.List;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;
import org.apache.pekko.japi.pf.PatternMatchReceive;

// #imports

// #advanced-actor
public class AdvancedPatternMatchActor extends AbstractActor {
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  public static class Request {
    public final String query;

    public Request(String query) {
      this.query = query;
    }
  }

  public static class Response {
    public final String result;

    public Response(String result) {
      this.result = result;
    }
  }

  @Override
  public Receive createReceive() {
    return PatternMatchReceive.create(
        msg -> {
          // Java 17+ compatible pattern matching
          if (msg instanceof String s) {
            if (s.startsWith("hello")) {
              log.info("Received greeting: {}", s);
              getSender().tell("Hello back!", getSelf());
            } else if (s.startsWith("error")) {
              log.error("Error message received: {}", s);
              getSender().tell(new RuntimeException(s), getSelf());
            } else {
              log.info("Received other string: {}", s);
              getSender().tell(s.toUpperCase(), getSelf());
            }
          } else if (msg instanceof Request req) {
            if (req.query != null && !req.query.isEmpty()) {
              log.info("Processing request: {}", req.query);
              getSender().tell(new Response("Processed: " + req.query), getSelf());
            } else {
              log.warning("Invalid request with empty query");
              getSender().tell(new Response("Error: Empty query"), getSelf());
            }
          } else if (msg instanceof List<?> list) {
            if (list.size() > 10) {
              log.info("Large list received with {} items", list.size());
              getSender().tell("Large list processed", getSelf());
            } else {
              log.info("Small list received with {} items", list.size());
              getSender().tell("Small list processed", getSelf());
            }
          } else if (msg instanceof Integer i) {
            if (i > 100) {
              log.info("Large number: {}", i);
              getSender().tell(i / 10, getSelf());
            } else if (i < 0) {
              log.warning("Negative number: {}", i);
              getSender().tell(Math.abs(i), getSelf());
            } else {
              log.info("Normal number: {}", i);
              getSender().tell(i * 2, getSelf());
            }
          } else {
            log.info("Unknown message type: {}", msg.getClass().getSimpleName());
            unhandled(msg);
          }
        });
  }
}
// #advanced-actor
