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
 * A modern alternative to {@link ReceiveBuilder} that leverages Java's pattern matching for cleaner
 * and more efficient message handling.
 *
 * <p>This class provides a way to create receive functions using Java's switch expressions and
 * pattern matching, reducing the need for complex ReceiveBuilder chains.
 *
 * <p>Example usage:
 *
 * <pre>
 * &#64;Override
 * public Receive createReceive() {
 *   // Java 21+ with pattern matching and guard clauses:
 *   return PatternMatchReceive.create(msg -> switch (msg) {
 *     case Double d -> {
 *       getSender().tell(d.isNaN() ? 0 : d, self());
 *     }
 *     case Integer i -> {
 *       getSender().tell(i * 10, self());
 *     }
 *     case String s when s.startsWith("foo") -> {
 *       getSender().tell(s.toUpperCase(), self());
 *     }
 *     default -> unhandled(msg);
 *   });
 *
 *   // Java 17+ compatible version:
 *   return PatternMatchReceive.create(msg -> {
 *     if (msg instanceof Double d) {
 *       getSender().tell(d.isNaN() ? 0 : d, self());
 *     } else if (msg instanceof Integer i) {
 *       getSender().tell(i * 10, self());
 *     } else if (msg instanceof String s && s.startsWith("foo")) {
 *       getSender().tell(s.toUpperCase(), self());
 *     } else {
 *       unhandled(msg);
 *     }
 *   });
 * }
 * </pre>
 */
public class PatternMatchReceive {

  /**
   * Create a {@link Receive} from a pattern matching procedure.
   *
   * @param handler a procedure that handles incoming messages using pattern matching
   * @return a Receive for the given handler
   */
  public static Receive create(Procedure<Object> handler) {
    PartialFunction<Object, BoxedUnit> pf =
        new PartialFunction<Object, BoxedUnit>() {
          @Override
          public boolean isDefinedAt(Object x) {
            return true; // Pattern matching will handle all cases including default
          }

          @Override
          public BoxedUnit apply(Object v1) {
            try {
              handler.apply(v1);
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

    return new Receive(pf);
  }

  /**
   * Create a {@link Receive} from a pattern matching procedure with explicit unhandled support.
   * This version allows for partial pattern matching where some messages are explicitly not
   * handled.
   *
   * @param handler a procedure that handles incoming messages using pattern matching
   * @param unhandledHandler a procedure to handle unhandled messages
   * @return a Receive for the given handlers
   */
  public static Receive create(Procedure<Object> handler, Procedure<Object> unhandledHandler) {
    PartialFunction<Object, BoxedUnit> pf =
        new PartialFunction<Object, BoxedUnit>() {
          @Override
          public boolean isDefinedAt(Object x) {
            return true;
          }

          @Override
          public BoxedUnit apply(Object v1) {
            try {
              handler.apply(v1);
              return BoxedUnit.UNIT;
            } catch (UnhandledException e) {
              try {
                unhandledHandler.apply(v1);
                return BoxedUnit.UNIT;
              } catch (Exception ex) {
                if (ex instanceof RuntimeException) {
                  throw (RuntimeException) ex;
                } else {
                  throw new RuntimeException(ex);
                }
              }
            } catch (Exception e) {
              if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
              } else {
                throw new RuntimeException(e);
              }
            }
          }
        };

    return new Receive(pf);
  }

  /**
   * Exception thrown to indicate a message was not handled in pattern matching. Can be used to fall
   * back to an unhandled message handler.
   */
  public static class UnhandledException extends RuntimeException {
    public UnhandledException() {
      super();
    }

    public UnhandledException(String message) {
      super(message);
    }
  }
}
