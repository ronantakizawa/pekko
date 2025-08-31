/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.japi.pf;

import org.apache.pekko.actor.AbstractActor.Receive;
import org.apache.pekko.japi.function.Procedure;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

/**
 * A high-performance alternative to {@link ReceiveBuilder} that leverages Java's pattern matching
 * for more efficient message handling.
 *
 * <p>This class replaces the sequential partial function evaluation of ReceiveBuilder with
 * optimized pattern matching that compiles to efficient bytecode.
 *
 * <p>Performance benefits over ReceiveBuilder:
 *
 * <ul>
 *   <li>Single switch statement vs. chain of partial function evaluations
 *   <li>JVM can optimize pattern matching to jump tables
 *   <li>Eliminates intermediate ReceiveBuilder objects and method calls
 * </ul>
 *
 * <p>Usage with Java 21+ switch expressions (recommended):
 *
 * <pre>
 * &#64;Override
 * public Receive createReceive() {
 *   return PatternMatchReceive.createOptimized(msg -> switch (msg) {
 *     case String s when s.startsWith("hello") -> {
 *       getSender().tell("Hello back!", getSelf());
 *       yield null;
 *     }
 *     case Integer i when i > 0 -> {
 *       getSender().tell(i * 2, getSelf());
 *       yield null;
 *     }
 *     case Double d -> {
 *       getSender().tell(d.isNaN() ? 0.0 : d, getSelf());
 *       yield null;
 *     }
 *     default -> {
 *       unhandled(msg);
 *       yield null;
 *     }
 *   });
 * }
 * </pre>
 *
 * <p>Usage with Java 17+ instanceof patterns (compatible):
 *
 * <pre>
 * &#64;Override
 * public Receive createReceive() {
 *   return PatternMatchReceive.create(msg -> {
 *     if (msg instanceof String s && s.startsWith("hello")) {
 *       getSender().tell("Hello back!", getSelf());
 *     } else if (msg instanceof Integer i && i > 0) {
 *       getSender().tell(i * 2, getSelf());
 *     } else if (msg instanceof Double d) {
 *       getSender().tell(d.isNaN() ? 0.0 : d, getSelf());
 *     } else {
 *       unhandled(msg);
 *     }
 *   });
 * }
 * </pre>
 */
public final class PatternMatchReceive {

  private PatternMatchReceive() {
    // Utility class
  }

  /**
   * Create a high-performance {@link Receive} using Java 21+ switch expressions.
   *
   * <p>This method provides the best performance by leveraging the JVM's optimized compilation of
   * switch expressions into jump tables.
   *
   * @param handler a function that handles messages using switch expressions
   * @return a Receive for the given handler
   */
  public static <T> Receive createOptimized(java.util.function.Function<Object, T> handler) {
    return new Receive(createOptimizedPartialFunction(handler));
  }

  /**
   * Create a {@link Receive} from a pattern matching procedure (Java 17+ compatible).
   *
   * <p>While compatible with older Java versions, this method is less optimal than {@link
   * #createOptimized(java.util.function.Function)} as it cannot leverage switch expression
   * optimizations.
   *
   * @param handler a procedure that handles incoming messages using pattern matching
   * @return a Receive for the given handler
   */
  public static Receive create(Procedure<Object> handler) {
    return new Receive(createPartialFunction(handler));
  }

  private static <T> PartialFunction<Object, BoxedUnit> createOptimizedPartialFunction(
      java.util.function.Function<Object, T> handler) {
    return new PartialFunction<Object, BoxedUnit>() {
      @Override
      public boolean isDefinedAt(Object x) {
        return true; // Pattern matching handles all cases including default
      }

      @Override
      public BoxedUnit apply(Object msg) {
        try {
          handler.apply(msg);
          return BoxedUnit.UNIT;
        } catch (Exception e) {
          if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  private static PartialFunction<Object, BoxedUnit> createPartialFunction(
      Procedure<Object> handler) {
    return new PartialFunction<Object, BoxedUnit>() {
      @Override
      public boolean isDefinedAt(Object x) {
        return true; // Pattern matching handles all cases including default
      }

      @Override
      public BoxedUnit apply(Object msg) {
        try {
          handler.apply(msg);
          return BoxedUnit.UNIT;
        } catch (Exception e) {
          if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else {
            throw new RuntimeException(e);
          }
        }
      }
    };
  }

  /**
   * Migration helper: Converts existing ReceiveBuilder chains to optimized pattern matching.
   *
   * <p>Usage:
   *
   * <pre>
   * // Instead of:
   * ReceiveBuilder.create()
   *   .match(String.class, s -> handleString(s))
   *   .match(Integer.class, i -> handleInteger(i))
   *   .matchAny(msg -> handleOther(msg))
   *   .build();
   *
   * // Use:
   * PatternMatchReceive.fromBuilder(msg -> switch (msg) {
   *   case String s -> { handleString(s); yield null; }
   *   case Integer i -> { handleInteger(i); yield null; }
   *   default -> { handleOther(msg); yield null; }
   * });
   * </pre>
   */
  public static <T> Receive fromBuilder(java.util.function.Function<Object, T> handler) {
    return createOptimized(handler);
  }
}
