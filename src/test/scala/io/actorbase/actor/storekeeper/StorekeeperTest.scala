package io.actorbase.actor.storekeeper

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Count, Get, Put, Remove}
import io.actorbase.actor.storekeeper.Storekeeper.Response._
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, MustMatchers, WordSpecLike}

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
  * Tests relative to the Storekeeper actor.
  *
  * @author Riccardo Cardin
  * @version 1.0
  * @since 1.0
  */
class StorekeeperTest extends TestKit(ActorSystem("testSystemStorekeeper"))
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfter
  with BeforeAndAfterAll {

  var sk: TestActorRef[Storekeeper] = _

  before {
    sk = TestActorRef[Storekeeper]
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A Storekeeper actor" must {
    "send back size 0 for an empty map" in {
      sk ! Count(1L)
      expectMsg(Size(0L, 1L))
    }

    "put a new couple (key, value) inside an empty map" in {
      sk ! Put("key", SerializationUtils.serialize(42), 1L)
      expectMsg(PutAck("key", 1L))
    }

    "receive an error message for a couple (null, value)" in {
      sk ! Put(null, SerializationUtils.serialize(42), 1L)
      expectMsg(PutNAck(null, "Key cannot be null", 1L))
    }

    "upsert the value of a key already in the map" in {
      sk ! Put("key", SerializationUtils.serialize(42), 1L)
      sk ! Put("key", SerializationUtils.serialize(18), 2L)
      sk ! Count(3L)
      expectMsgAllOf(PutAck("key", 1L), PutAck("key", 2L), Size(1, 3L))
    }

    "be able to put two different keys in the same map" in {
      sk ! Put("key1", SerializationUtils.serialize(42), 1L)
      sk ! Put("key2", SerializationUtils.serialize(18), 2L)
      sk ! Count(3L)
      expectMsgAllOf(PutAck("key1", 1L), PutAck("key2", 2L), Size(2, 3L))
    }

    "get the value of a key just inserted" in {
      val value = SerializationUtils.serialize(42)
      sk ! Put("key1", value, 1L)
      sk ! Get("key1", 2L)
      sk ! Count(3L)
      expectMsgAllOf(PutAck("key1", 1L), Item("key1", Some((value, 1L)), 2L), Size(1, 3L))
    }

    "get a None if map is empty" in {
      sk ! Get("key", 1L)
      expectMsg(Item("key", None, 1L))
    }

    "get a None if map does not contain the key" in {
      sk ! Put("key", SerializationUtils.serialize(42), 1L)
      sk ! Get("key1", 2L)
      expectMsgAllOf(PutAck("key", 1L), Item("key1", None, 2L))
    }

    "remove the value of a key present in the map" in {
      sk ! Put("key", SerializationUtils.serialize(42), 1L)
      sk ! Remove("key", 2L)
      sk ! Count(3L)
      expectMsgAllOf(PutAck("key", 1L), RemoveAck("key", 2L), Size(0, 3L))
    }

    "receive an ack if the map is empty" in {
      sk ! Remove("key", 1L)
      expectMsgAllOf(RemoveAck("key", 1L))
    }

    "receive an ack if the map does not contain the key to be removed" in {
      sk ! Put("key", SerializationUtils.serialize(42), 1L)
      sk ! Remove("key1", 2L)
      sk ! Count(3L)
      expectMsgAllOf(PutAck("key", 1L), RemoveAck("key1", 2L), Size(1, 3L))
    }
  }
}
