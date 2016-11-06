package io.actorbase.actor.storekeeper

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Count, Put}
import io.actorbase.actor.storekeeper.Storekeeper.Response.{PutAck, PutNAck, Size}
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec, WordSpecLike}

import scala.util.{Failure, Success}

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
  * Please, insert description here.
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
      sk ! Count
      expectMsg(Size(0L))
    }

    "put a new couple (key, value) inside an empty map" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42))
      expectMsg(PutAck("key"))
    }

    "receive an error message for a couple (null, value)" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put(null, SerializationUtils.serialize(42))
      expectMsg(PutNAck("Key cannot be null"))
    }

    "upsert the value of a key already in the map" in {
      val sk = TestActorRef[Storekeeper]
      sk ! Put("key", SerializationUtils.serialize(42))
      sk ! Put("key", SerializationUtils.serialize(18))
      sk ! Count
      expectMsgAllOf(PutAck("key"), PutAck("key"), Size(1))
    }
  }
}
