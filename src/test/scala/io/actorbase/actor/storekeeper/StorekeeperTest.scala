package io.actorbase.actor.storekeeper

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Count, Get, Put, Remove}
import io.actorbase.actor.storekeeper.Storekeeper.Response._
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

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
  with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A Storekeeper actor" must {
    "send back size 0 for an empty map" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Count("uuid")
      expectMsg(Size(0L, "uuid"))
    }

    "put a new couple (key, value) inside an empty map" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42), "uuid")
      expectMsg(PutAck("key", "uuid"))
    }

    "receive an error message for a couple (null, value)" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42), "uuid")
      expectMsg(PutNAck("key", "Key cannot be null", "uuid"))
    }

    "upsert the value of a key already in the map" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42), "uuid")
      sk ! Put("key", SerializationUtils.serialize(18), "uuid")
      sk ! Count("uuid")
      expectMsgAllOf(PutAck("key", "uuid"), PutAck("key", "uuid"), Size(1, "uuid"))
    }

    "be able to put two different keys in the same map" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key1", SerializationUtils.serialize(42), "uuid")
      sk ! Put("key2", SerializationUtils.serialize(18), "uuid")
      sk ! Count("uuid")
      expectMsgAllOf(PutAck("key1", "uuid"), PutAck("key2", "uuid"), Size(2, "uuid"))
    }

    "get the value of a key just inserted" in {
      val sk = TestActorRef[Storekeeper]
      val value = SerializationUtils.serialize(42)
      sk ! Put("key1", value, "uuid")
      sk ! Get("key1", "uuid")
      sk ! Count("uuid")
      expectMsgAllOf(PutAck("key1", "uuid"), Item("key1", Some(value), "uuid"), Size(1, "uuid"))
    }

    "get a None if map is empty" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Get("key", "uuid")
      expectMsg(Item("key", None, "uuid"))
    }

    "get a None if map does not contain the key" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42), "uuid")
      sk ! Get("key1", "uuid")
      expectMsgAllOf(PutAck("key", "uuid"), Item("key1", None, "uuid"))
    }

    "remove the value of a key present in the map" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42), "uuid")
      sk ! Remove("key", "uuid")
      sk ! Count("uuid")
      expectMsgAllOf(PutAck("key", "uuid"), RemoveAck("key", "uuid"), Size(0, "uuid"))
    }

    "receive an ack if the map is empty" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Remove("key", "uuid")
      expectMsgAllOf(RemoveAck("key", "uuid"))
    }

    "receive an ack if the map does not contain the key to be removed" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42), "uuid")
      sk ! Remove("key1", "uuid")
      sk ! Count("uuid")
      expectMsgAllOf(PutAck("key", "uuid"), RemoveAck("key1", "uuid"), Size(1, "uuid"))
    }
  }
}
