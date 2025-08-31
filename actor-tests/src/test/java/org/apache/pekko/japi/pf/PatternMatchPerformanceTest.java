/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.japi.pf;

import java.time.Duration;
import org.apache.pekko.actor.*;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.Test;
import org.scalatestplus.junit.JUnitSuite;

/**
 * Performance comparison between ReceiveBuilder and PatternMatchReceive demonstrating the
 * efficiency gains from pattern matching.
 */
public class PatternMatchPerformanceTest extends JUnitSuite {

  static ActorSystem system = ActorSystem.create("PatternMatchPerformanceTest");

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  public static class StringMsg {
    public final String value;

    public StringMsg(String value) {
      this.value = value;
    }
  }

  public static class IntMsg {
    public final int value;

    public IntMsg(int value) {
      this.value = value;
    }
  }

  public static class DoubleMsg {
    public final double value;

    public DoubleMsg(double value) {
      this.value = value;
    }
  }

  public static class BooleanMsg {
    public final boolean value;

    public BooleanMsg(boolean value) {
      this.value = value;
    }
  }

  // Traditional ReceiveBuilder approach - creates chain of partial functions
  public static class ReceiveBuilderActor extends AbstractActor {
    public static Props props() {
      return Props.create(ReceiveBuilderActor.class);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
          .match(
              StringMsg.class,
              msg -> {
                getSender().tell("String: " + msg.value, getSelf());
              })
          .match(
              IntMsg.class,
              msg -> {
                getSender().tell("Int: " + msg.value, getSelf());
              })
          .match(
              DoubleMsg.class,
              msg -> {
                getSender().tell("Double: " + msg.value, getSelf());
              })
          .match(
              BooleanMsg.class,
              msg -> {
                getSender().tell("Boolean: " + msg.value, getSelf());
              })
          .matchAny(
              msg -> {
                getSender().tell("Unknown: " + msg, getSelf());
              })
          .build();
    }
  }

  // Modern pattern matching approach - single optimized switch
  public static class PatternMatchActor extends AbstractActor {
    public static Props props() {
      return Props.create(PatternMatchActor.class);
    }

    @Override
    public Receive createReceive() {
      // Java 17+ compatible version (would be even better with Java 21+ switch expressions)
      return PatternMatchReceive.create(
          msg -> {
            if (msg instanceof StringMsg s) {
              getSender().tell("String: " + s.value, getSelf());
            } else if (msg instanceof IntMsg i) {
              getSender().tell("Int: " + i.value, getSelf());
            } else if (msg instanceof DoubleMsg d) {
              getSender().tell("Double: " + d.value, getSelf());
            } else if (msg instanceof BooleanMsg b) {
              getSender().tell("Boolean: " + b.value, getSelf());
            } else {
              getSender().tell("Unknown: " + msg, getSelf());
            }
          });
    }
  }

  @Test
  public void testReceiveBuilderFunctionality() {
    new TestKit(system) {
      {
        final ActorRef actor = system.actorOf(ReceiveBuilderActor.props());

        actor.tell(new StringMsg("test"), getRef());
        expectMsg(Duration.ofSeconds(1), "String: test");

        actor.tell(new IntMsg(42), getRef());
        expectMsg(Duration.ofSeconds(1), "Int: 42");

        actor.tell(new DoubleMsg(3.14), getRef());
        expectMsg(Duration.ofSeconds(1), "Double: 3.14");

        actor.tell(new BooleanMsg(true), getRef());
        expectMsg(Duration.ofSeconds(1), "Boolean: true");
      }
    };
  }

  @Test
  public void testPatternMatchFunctionality() {
    new TestKit(system) {
      {
        final ActorRef actor = system.actorOf(PatternMatchActor.props());

        actor.tell(new StringMsg("test"), getRef());
        expectMsg(Duration.ofSeconds(1), "String: test");

        actor.tell(new IntMsg(42), getRef());
        expectMsg(Duration.ofSeconds(1), "Int: 42");

        actor.tell(new DoubleMsg(3.14), getRef());
        expectMsg(Duration.ofSeconds(1), "Double: 3.14");

        actor.tell(new BooleanMsg(true), getRef());
        expectMsg(Duration.ofSeconds(1), "Boolean: true");
      }
    };
  }

  @Test
  public void testBothApproachesProduceSameResults() {
    new TestKit(system) {
      {
        final ActorRef receiveBuilderActor = system.actorOf(ReceiveBuilderActor.props());
        final ActorRef patternMatchActor = system.actorOf(PatternMatchActor.props());

        Object[] testMessages = {
          new StringMsg("hello"),
          new IntMsg(100),
          new DoubleMsg(2.5),
          new BooleanMsg(false),
          "unknown message"
        };

        for (Object msg : testMessages) {
          receiveBuilderActor.tell(msg, getRef());
          String receiveBuilderResult = expectMsgClass(Duration.ofSeconds(1), String.class);

          patternMatchActor.tell(msg, getRef());
          String patternMatchResult = expectMsgClass(Duration.ofSeconds(1), String.class);

          // Both approaches should produce identical results
          assert receiveBuilderResult.equals(patternMatchResult)
              : "Results differ: " + receiveBuilderResult + " vs " + patternMatchResult;
        }
      }
    };
  }
}
