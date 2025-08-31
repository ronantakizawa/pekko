/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package jdocs.actor;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.event.Logging;
import org.apache.pekko.event.LoggingAdapter;
import org.apache.pekko.japi.pf.PatternMatchReceive;
import org.apache.pekko.japi.pf.ReceiveBuilder;

/**
 * Example showing migration from ReceiveBuilder to modern pattern matching. This demonstrates the
 * benefits of using Java's new pattern matching features instead of the traditional ReceiveBuilder
 * approach.
 */
public class PatternMatchingMigrationExample extends AbstractActor {
  private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

  public static class Message {
    public final String content;
    public final int priority;

    public Message(String content, int priority) {
      this.content = content;
      this.priority = priority;
    }
  }

  // #old-receive-builder
  // Traditional ReceiveBuilder approach - verbose and less efficient
  public Receive createReceiveOld() {
    return ReceiveBuilder.create()
        .match(
            Double.class,
            d -> {
              getSender().tell(d.isNaN() ? 0 : d, getSelf());
            })
        .match(
            Integer.class,
            i -> {
              getSender().tell(i * 10, getSelf());
            })
        .match(
            String.class,
            s -> s.startsWith("foo"),
            s -> {
              getSender().tell(s.toUpperCase(), getSelf());
            })
        .match(
            String.class,
            s -> s.startsWith("error"),
            s -> {
              log.error("Error message: {}", s);
              getSender().tell("ERROR", getSelf());
            })
        .match(
            String.class,
            s -> {
              getSender().tell(s, getSelf());
            })
        .match(
            Message.class,
            msg -> msg.priority > 5,
            msg -> {
              log.info("High priority message: {}", msg.content);
              getSender().tell("HIGH: " + msg.content, getSelf());
            })
        .match(
            Message.class,
            msg -> {
              log.info("Normal message: {}", msg.content);
              getSender().tell("NORMAL: " + msg.content, getSelf());
            })
        .matchAny(
            msg -> {
              log.warning("Unknown message: {}", msg);
              unhandled(msg);
            })
        .build();
  }

  // #old-receive-builder

  // #new-pattern-matching
  // Modern pattern matching approach - cleaner and more efficient
  public Receive createReceiveNew() {
    return PatternMatchReceive.create(
        msg -> {
          switch (msg) {
            case Double d -> {
              getSender().tell(d.isNaN() ? 0 : d, getSelf());
            }
            case Integer i -> {
              getSender().tell(i * 10, getSelf());
            }
            case String s when s.startsWith("foo") -> {
              getSender().tell(s.toUpperCase(), getSelf());
            }
            case String s when s.startsWith("error") -> {
              log.error("Error message: {}", s);
              getSender().tell("ERROR", getSelf());
            }
            case String s -> {
              getSender().tell(s, getSelf());
            }
            case Message m when m.priority > 5 -> {
              log.info("High priority message: {}", m.content);
              getSender().tell("HIGH: " + m.content, getSelf());
            }
            case Message m -> {
              log.info("Normal message: {}", m.content);
              getSender().tell("NORMAL: " + m.content, getSelf());
            }
            default -> {
              log.warning("Unknown message: {}", msg);
              unhandled(msg);
            }
          }
        });
  }

  // #new-pattern-matching

  @Override
  public Receive createReceive() {
    // Use the modern pattern matching version
    return createReceiveNew();
  }
}

/**
 * Benefits of pattern matching over ReceiveBuilder:
 *
 * <p>1. Performance: Single switch statement vs. chain of partial function calls 2. Readability:
 * More familiar syntax, cleaner structure 3. Guard conditions: Native 'when' clauses instead of
 * separate predicate functions 4. Type safety: Better compile-time checking with pattern matching
 * 5. Maintainability: Less boilerplate, easier to modify and extend 6. Modern Java: Leverages Java
 * 21+ features for better developer experience
 */
