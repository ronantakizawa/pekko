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

public class PatternMatchReceiveTest extends JUnitSuite {

  static ActorSystem system = ActorSystem.create("PatternMatchReceiveTest");

  @AfterClass
  public static void teardown() {
    TestKit.shutdownActorSystem(system);
    system = null;
  }

  public static class TestActor extends AbstractActor {
    public static Props props() {
      return Props.create(TestActor.class);
    }

    @Override
    public Receive createReceive() {
      return PatternMatchReceive.create(
          msg -> {
            // Java 17 compatible pattern matching without guard clauses
            if (msg instanceof String s) {
              if (s.startsWith("hello")) {
                getSender().tell("Hello back!", getSelf());
              } else {
                getSender().tell(s.toUpperCase(), getSelf());
              }
            } else if (msg instanceof Integer i) {
              if (i > 0) {
                getSender().tell(i * 2, getSelf());
              } else {
                getSender().tell("negative", getSelf());
              }
            } else if (msg instanceof Double d) {
              getSender().tell(d.isNaN() ? 0.0 : d, getSelf());
            } else {
              getSender().tell("unknown", getSelf());
            }
          });
    }
  }

  @Test
  public void testStringPatternMatching() {
    new TestKit(system) {
      {
        final ActorRef actor = system.actorOf(TestActor.props());

        actor.tell("hello world", getRef());
        expectMsg(Duration.ofSeconds(1), "Hello back!");

        actor.tell("test", getRef());
        expectMsg(Duration.ofSeconds(1), "TEST");
      }
    };
  }

  @Test
  public void testIntegerPatternMatching() {
    new TestKit(system) {
      {
        final ActorRef actor = system.actorOf(TestActor.props());

        actor.tell(5, getRef());
        expectMsg(Duration.ofSeconds(1), 10);

        actor.tell(-3, getRef());
        expectMsg(Duration.ofSeconds(1), "negative");
      }
    };
  }

  @Test
  public void testDoublePatternMatching() {
    new TestKit(system) {
      {
        final ActorRef actor = system.actorOf(TestActor.props());

        actor.tell(3.14, getRef());
        expectMsg(Duration.ofSeconds(1), 3.14);

        actor.tell(Double.NaN, getRef());
        expectMsg(Duration.ofSeconds(1), 0.0);
      }
    };
  }

  @Test
  public void testUnknownMessageHandling() {
    new TestKit(system) {
      {
        final ActorRef actor = system.actorOf(TestActor.props());

        actor.tell(new Object(), getRef());
        expectMsg(Duration.ofSeconds(1), "unknown");
      }
    };
  }
}
