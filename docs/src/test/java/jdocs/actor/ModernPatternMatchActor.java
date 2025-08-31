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
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;
import org.apache.pekko.japi.pf.PatternMatchReceive;

// #imports

// #modern-actor
public class ModernPatternMatchActor extends AbstractActor {
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  @Override
  public Receive createReceive() {
    return PatternMatchReceive.create(
        msg -> {
          // Java 17+ compatible pattern matching
          if (msg instanceof String s) {
            log.info("Received String message: {}", s);
            getSender().tell(s, getSelf());
          } else if (msg instanceof Integer i) {
            log.info("Received Integer message: {}", i);
            getSender().tell(i * 2, getSelf());
          } else if (msg instanceof Double d) {
            log.info("Received Double message: {}", d);
            getSender().tell(d.isNaN() ? 0.0 : d, getSelf());
          } else {
            log.info("received unknown message: {}", msg);
            unhandled(msg);
          }
        });
  }
}
// #modern-actor
