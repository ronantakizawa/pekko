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
 * Comprehensive example showing migration from ReceiveBuilder to PatternMatchReceive. This
 * demonstrates how to reduce ReceiveBuilder usage and leverage Java's pattern matching for better
 * performance.
 */
public class ReceiveBuilderMigrationExample {

  public static class StartMessage {
    public final String name;

    public StartMessage(String name) {
      this.name = name;
    }
  }

  public static class CountMessage {
    public final int count;

    public CountMessage(int count) {
      this.count = count;
    }
  }

  public static class StopMessage {}

  // #before-migration
  // BEFORE: Traditional ReceiveBuilder - creates sequential partial function chain
  public static class OldReceiveBuilderActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private int messageCount = 0;

    @Override
    public Receive createReceive() {
      // This creates a chain of partial functions that are evaluated sequentially:
      // 1. Check if StartMessage -> handle or continue
      // 2. Check if CountMessage -> handle or continue
      // 3. Check if StopMessage -> handle or continue
      // 4. Check if String -> handle or continue
      // 5. Handle any remaining message
      return ReceiveBuilder.create()
          .match(
              StartMessage.class,
              msg -> {
                log.info("Starting with name: {}", msg.name);
                messageCount = 0;
                getSender().tell("Started: " + msg.name, getSelf());
              })
          .match(
              CountMessage.class,
              msg -> {
                messageCount += msg.count;
                log.info("Count increased by {}, total: {}", msg.count, messageCount);
                getSender().tell("Count: " + messageCount, getSelf());
              })
          .match(
              StopMessage.class,
              msg -> {
                log.info("Stopping with final count: {}", messageCount);
                getSender().tell("Stopped: " + messageCount, getSelf());
              })
          .match(
              String.class,
              str -> str.startsWith("cmd:"),
              str -> {
                log.info("Command received: {}", str);
                getSender().tell("Command processed: " + str, getSelf());
              })
          .match(
              String.class,
              str -> {
                log.info("String message: {}", str);
                getSender().tell("Echo: " + str, getSelf());
              })
          .matchAny(
              msg -> {
                log.warning("Unknown message type: {}", msg.getClass().getSimpleName());
                unhandled(msg);
              })
          .build();
    }
  }

  // #before-migration

  // #after-migration
  // AFTER: Modern pattern matching - single optimized switch
  public static class NewPatternMatchActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private int messageCount = 0;

    @Override
    public Receive createReceive() {
      // This compiles to a single efficient switch that can be optimized
      // by the JVM into jump tables or decision trees
      return PatternMatchReceive.create(
          msg -> {
            if (msg instanceof StartMessage start) {
              log.info("Starting with name: {}", start.name);
              messageCount = 0;
              getSender().tell("Started: " + start.name, getSelf());

            } else if (msg instanceof CountMessage count) {
              messageCount += count.count;
              log.info("Count increased by {}, total: {}", count.count, messageCount);
              getSender().tell("Count: " + messageCount, getSelf());

            } else if (msg instanceof StopMessage) {
              log.info("Stopping with final count: {}", messageCount);
              getSender().tell("Stopped: " + messageCount, getSelf());

            } else if (msg instanceof String str && str.startsWith("cmd:")) {
              log.info("Command received: {}", str);
              getSender().tell("Command processed: " + str, getSelf());

            } else if (msg instanceof String str) {
              log.info("String message: {}", str);
              getSender().tell("Echo: " + str, getSelf());

            } else {
              log.warning("Unknown message type: {}", msg.getClass().getSimpleName());
              unhandled(msg);
            }
          });
    }
  }

  // #after-migration

  // #java21-optimized
  // BEST: Java 21+ switch expressions (requires Java 21+)
  public static class OptimizedPatternMatchActor extends AbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private int messageCount = 0;

    @Override
    public Receive createReceive() {
      // Note: This would use Java 21+ switch expressions in real implementation
      // Shown here as comment since project uses Java 17

      /*
      return PatternMatchReceive.createOptimized(msg -> switch (msg) {
        case StartMessage start -> {
          log.info("Starting with name: {}", start.name);
          messageCount = 0;
          getSender().tell("Started: " + start.name, getSelf());
          yield null;
        }
        case CountMessage count -> {
          messageCount += count.count;
          log.info("Count increased by {}, total: {}", count.count, messageCount);
          getSender().tell("Count: " + messageCount, getSelf());
          yield null;
        }
        case StopMessage stop -> {
          log.info("Stopping with final count: {}", messageCount);
          getSender().tell("Stopped: " + messageCount, getSelf());
          yield null;
        }
        case String str when str.startsWith("cmd:") -> {
          log.info("Command received: {}", str);
          getSender().tell("Command processed: " + str, getSelf());
          yield null;
        }
        case String str -> {
          log.info("String message: {}", str);
          getSender().tell("Echo: " + str, getSelf());
          yield null;
        }
        default -> {
          log.warning("Unknown message type: {}", msg.getClass().getSimpleName());
          unhandled(msg);
          yield null;
        }
      });
      */

      // Java 17 compatible version
      return PatternMatchReceive.create(
          msg -> {
            if (msg instanceof StartMessage start) {
              log.info("Starting with name: {}", start.name);
              messageCount = 0;
              getSender().tell("Started: " + start.name, getSelf());
            } else if (msg instanceof CountMessage count) {
              messageCount += count.count;
              log.info("Count increased by {}, total: {}", count.count, messageCount);
              getSender().tell("Count: " + messageCount, getSelf());
            } else if (msg instanceof StopMessage) {
              log.info("Stopping with final count: {}", messageCount);
              getSender().tell("Stopped: " + messageCount, getSelf());
            } else if (msg instanceof String str && str.startsWith("cmd:")) {
              log.info("Command received: {}", str);
              getSender().tell("Command processed: " + str, getSelf());
            } else if (msg instanceof String str) {
              log.info("String message: {}", str);
              getSender().tell("Echo: " + str, getSelf());
            } else {
              log.warning("Unknown message type: {}", msg.getClass().getSimpleName());
              unhandled(msg);
            }
          });
    }
  }
  // #java21-optimized
}

/**
 * Performance comparison:
 *
 * <p>ReceiveBuilder approach: - Creates 6 separate partial functions - Sequential evaluation: msg
 * -> PF1 -> PF2 -> PF3 -> PF4 -> PF5 -> PF6 - Each partial function calls isDefinedAt() then
 * apply() - Overhead from ReceiveBuilder object construction - Chain of method calls that can't be
 * optimized well by JVM
 *
 * <p>Pattern matching approach: - Single conditional chain that compiles to efficient bytecode -
 * JVM can optimize instanceof checks and branch prediction - Direct method calls without partial
 * function overhead - Pattern variables eliminate casting - Better memory usage (no intermediate
 * objects)
 *
 * <p>Java 21+ switch expressions: - Optimal compilation to jump tables or decision trees - Guard
 * clauses (when) compile to efficient checks - Best possible performance for message dispatch
 */
