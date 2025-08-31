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
 * A more efficient alternative to {@link ReceiveBuilder} that uses Java's pattern matching
 * capabilities to reduce the overhead of partial function chains.
 *
 * <p>This class provides better performance than ReceiveBuilder by avoiding the creation
 * of multiple partial function objects and their sequential evaluation.
 *
 * <p>Performance improvement over ReceiveBuilder:
 * <ul>
 *   <li>Single PartialFunction instead of chained PartialFunction.orElse() calls</li>
 *   <li>Direct pattern matching in user code instead of individual isDefinedAt checks</li>
 *   <li>Eliminates intermediate objects created by ReceiveBuilder</li>
 * </ul>
 *
 * <p>Usage example (Java 17+):
 *
 * <pre>
 * &#64;Override
 * public Receive createReceive() {
 *   return PatternMatchReceive.create(msg -> {
 *     if (msg instanceof String s) {
 *       getSender().tell("String: " + s, getSelf());
 *     } else if (msg instanceof Integer i) {
 *       getSender().tell("Integer: " + i, getSelf());
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
   * Create a {@link Receive} from a message handling procedure using pattern matching.
   *
   * <p>This method creates a single PartialFunction that delegates to user-provided
   * pattern matching code, avoiding the overhead of ReceiveBuilder's chained partial functions.
   *
   * @param handler a procedure that handles incoming messages using pattern matching
   * @return a Receive for the given handler
   */
  public static Receive create(Procedure<Object> handler) {
    return new Receive(new PartialFunction<Object, BoxedUnit>() {
      @Override
      public boolean isDefinedAt(Object x) {
        // Pattern matching handles all cases including unhandled messages
        return true;
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
    });
  }
}