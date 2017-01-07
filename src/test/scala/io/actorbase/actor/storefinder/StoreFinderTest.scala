package io.actorbase.actor.storefinder

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.actorbase.actor.storefinder.StoreFinder.Request.{Count, Delete, Query, Upsert}
import io.actorbase.actor.storefinder.StoreFinder.Response._
import org.apache.commons.lang3.SerializationUtils
import org.scalatest._

import scala.concurrent.duration._

/**
  * The MIT License (MIT)
  *
  * Copyright (c) 2015 Riccardo Cardin
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */

/**
  * Tests relative to StoreFinder actor
  *
  * @author Riccardo Cardin
  * @version 1.0
  * @since 1.0
  */
class StoreFinderTest extends TestKit(ActorSystem("testSystemStoreFinder"))
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfter
  with BeforeAndAfterAll {

  val Payload: Array[Byte] = SerializationUtils.serialize(42)

  var sf: TestActorRef[StoreFinder] = _

  before {
    sf = TestActorRef(new StoreFinder("table"))
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An empty StoreFinder actor" must {
    "send back size 0" in {
      sf ! Count
      expectMsg(CountAck(0L))
    }

    "send back an empty ack to a query" in {
      sf ! Query("key")
      expectMsg(QueryAck("key", None))
    }

    "send an ack to a request of deletion" in {
      sf ! Delete("key")
      expectMsg(DeleteAck("key"))
    }

    "send an ack relative to the upsertion a couple (key, value)" in {
      sf ! Upsert("key", Payload)
      expectMsg(UpsertAck("key"))
    }

    "send an error if the key is null" in {
      sf ! Upsert(null, Payload)
      expectMsg(UpsertNAck(null, "Key cannot be null"))
    }
  }

  "A non empty StoreFinder actor" must {
    "accept different upserts for different keys" in {
      sf ! Upsert("key", Payload)
      expectMsg(UpsertAck("key"))
      sf ! Upsert("key1", SerializationUtils.serialize(4242))
      expectMsg(UpsertAck("key1"))
    }

    "send an error if the key is null" in {
      sf ! Upsert(null, Payload)
      expectMsg(UpsertNAck(null, "Key cannot be null"))
      sf ! Upsert("key", Payload)
      expectMsg(UpsertAck("key"))
    }
  }
}
