package io.actorbase.actor.storefinder

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.actorbase.actor.storefinder.StoreFinder.Request.{Count, Delete, Query, Upsert}
import io.actorbase.actor.storefinder.StoreFinder.Response._
import org.apache.commons.lang3.SerializationUtils
import org.scalatest._

/**
  * The MIT License (MIT)
  *
  * Copyright (c) 2015 - 2017 Riccardo Cardin
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
  * @version 0.1
  * @since 0.1
  */
class StoreFinderTest extends TestKit(ActorSystem("testSystemStoreFinder"))
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfter
  with BeforeAndAfterAll {

  val Payload: Array[Byte] = SerializationUtils.serialize(42)
  val Payload1: Array[Byte] = SerializationUtils.serialize(4242)
  val Uuid: Long = 1L
  val Uuid1: Long = 2L

  var sf: TestActorRef[StoreFinder] = _

  before {
    sf = TestActorRef(new StoreFinder("table"))
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "An empty StoreFinder actor" must {
    "send back size 0" in {
      sf ! Count(Uuid)
      expectMsg(CountAck(0L, Uuid))
    }

    "send back an empty ack to a query" in {
      sf ! Query("key", Uuid)
      expectMsg(QueryAck("key", None, Uuid))
    }

    "send an ack to a request of deletion" in {
      sf ! Delete("key", Uuid)
      expectMsg(DeleteAck("key", Uuid))
    }

    "send an ack relative to the upsertion a couple (key, value)" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
    }

    "send an error if the key is null" in {
      sf ! Upsert(null, Payload, Uuid)
      expectMsg(UpsertNAck(null, "Key cannot be null", Uuid))
    }
  }

  "A non empty StoreFinder actor" must {
    "accept different upserts for different keys" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Upsert("key1", Payload1, Uuid1)
      expectMsg(UpsertAck("key1", Uuid1))
    }

    "send an error if the key is null" in {
      sf ! Upsert(null, Payload, Uuid)
      expectMsg(UpsertNAck(null, "Key cannot be null", Uuid))
      sf ! Upsert("key", Payload, Uuid1)
      expectMsg(UpsertAck("key", Uuid1))
    }

    "accept different upserts for the same keys" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Upsert("key", Payload1, Uuid1)
      expectMsg(UpsertAck("key", Uuid1))
    }

    "count a single item" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Count(Uuid)
      expectMsg(CountAck(1L, Uuid))
    }

    "count a multiple items" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Upsert("key1", Payload, Uuid1)
      expectMsg(UpsertAck("key1", Uuid1))
      sf ! Count(Uuid)
      expectMsg(CountAck(2L, Uuid))
    }

    "not count upserts that receives a nack" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Upsert(null, Payload, Uuid1)
      expectMsg(UpsertNAck(null, "Key cannot be null", Uuid1))
      sf ! Count(Uuid)
      expectMsg(CountAck(1L, Uuid))
    }

    // FIXME This test will be available with the development of the normalization process
    "count a single item for multiple upserts of the same key" taggedAs Tag(classOf[Ignore].getName) in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Upsert("key", Payload, Uuid1)
      expectMsg(UpsertAck("key", Uuid1))
      sf ! Count(Uuid)
      expectMsg(CountAck(1L, Uuid))
    }

    "get a previous upserted item in an empty table" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Query("key", Uuid)
      expectMsg(QueryAck("key", Option(Payload), Uuid))
    }

    "get a none for a key not present" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Query("key1", Uuid)
      expectMsg(QueryAck("key1", None, Uuid))
    }

    "get a previous upserted item in a table containing more than one item" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Upsert("key1", Payload1, Uuid1)
      expectMsg(UpsertAck("key1", Uuid1))
      sf ! Query("key", Uuid)
      expectMsg(QueryAck("key", Option(Payload), Uuid))
    }

    "send an ack relative to the deletion of a previously inserted key" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Delete("key", Uuid)
      expectMsg(DeleteAck("key", Uuid))
      sf ! Query("key", Uuid)
      expectMsg(QueryAck("key", None, Uuid))
      sf ! Count(Uuid)
      expectMsg(CountAck(0, Uuid))
    }

    "send an ack relative to the deletion of a previously inserted key (more than one key present)" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Upsert("key1", Payload1, Uuid1)
      expectMsg(UpsertAck("key1", Uuid1))
      sf ! Delete("key", Uuid)
      expectMsg(DeleteAck("key", Uuid))
      sf ! Query("key", Uuid)
      expectMsg(QueryAck("key", None, Uuid))
      sf ! Query("key1", Uuid1)
      expectMsg(QueryAck("key1", Some(Payload1), Uuid1))
      sf ! Count(Uuid)
      expectMsg(CountAck(1, Uuid))
    }

    "send an ack relative to the deletion of a previously inserted key and insert another key" in {
      sf ! Upsert("key", Payload, Uuid)
      expectMsg(UpsertAck("key", Uuid))
      sf ! Delete("key", Uuid)
      expectMsg(DeleteAck("key", Uuid))
      sf ! Upsert("key1", Payload1, Uuid1)
      expectMsg(UpsertAck("key1", Uuid1))
      sf ! Query("key", Uuid)
      expectMsg(QueryAck("key", None, Uuid))
      sf ! Query("key1", Uuid1)
      expectMsg(QueryAck("key1", Some(Payload1), Uuid1))
      sf ! Count(Uuid)
      expectMsg(CountAck(1, Uuid))
    }
  }
}
