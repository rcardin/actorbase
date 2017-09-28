package io.actorbase.actor.main

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.actorbase.actor.api.Api.Request._
import io.actorbase.actor.api.Api.Response._
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, MustMatchers, WordSpecLike}

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
  *
  * Tests relative to an empty Actorbase (main) actor.
  *
  * @author Riccardo Cardin
  * @version 0.1
  * @since 0.1
  */
class EmptyActorbaseTest extends TestKit(ActorSystem("testSystemActorbase"))
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfter
  with BeforeAndAfterAll {

  var ab: TestActorRef[Actorbase] = _

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  before {
    ab = TestActorRef(new Actorbase)
  }

  "An empty database" must {
    "create a new table with a name" in {
      ab ! CreateCollection("table")
      expectMsg(CreateCollectionAck("table"))
    }

    "send an error for any find request" in {
      ab ! Find("table", "key")
      expectMsg(FindNAck("table", "key", "Collection table does not exist"))
    }

    "send an error for any upsert request" in {
      ab ! Upsert("table", "key", Array())
      expectMsg(UpsertNAck("table", "key", "Collection table does not exist"))
    }

    "send an error for any deletion request" in {
      ab ! Delete("table", "key")
      expectMsg(DeleteNAck("table", "key", "Collection table does not exist"))
    }

    "send an error for any count request" in {
      ab ! Count("table")
      expectMsg(CountNAck("table", "Collection table does not exist"))
    }
  }
}

/**
  * Tests relative to a not empty Actorbase (main) actor.
  *
  * @author Riccardo Cardin
  * @version 0.1
  * @since 0.1
  */
class ActorbaseTest extends TestKit(ActorSystem("testSystemActorbase"))
  with ImplicitSender
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfter
  with BeforeAndAfterAll {

  val Payload: Array[Byte] = SerializationUtils.serialize(42)
  val Payload1: Array[Byte] = SerializationUtils.serialize(4242)

  var ab: TestActorRef[Actorbase] = _

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  before {
    ab = TestActorRef(new Actorbase)
    createCollection("table")
  }

  "An non empty database" must {

    "be able to create two different collections" in {
      ab ! CreateCollection("table1")
      expectMsg(CreateCollectionAck("table1"))
    }

    "send an error if it tries to create the same collection more than once" in {
      ab ! CreateCollection("table")
      expectMsg(CreateCollectionNAck("table", "Collection table already exists"))
    }

    "upsert some information into an existing collection" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Upsert("table", "key1", Payload)
      expectMsg(UpsertAck("table", "key1"))
      ab ! Upsert("table", "key", Payload1)
      expectMsg(UpsertAck("table", "key"))
    }

    "send an error in case of upsertion to an inexistent collection" in {
      ab ! Upsert("table1", "key", Payload)
      expectMsg(UpsertNAck("table1", "key", "Collection table1 does not exist"))
    }

    "send an error if the upsertion key is null" in {
      ab ! Upsert("table", null, Payload)
      expectMsg(UpsertNAck("table", null, "Key cannot be null"))
    }

    "count a single item in a collection" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Count("table")
      expectMsg(CountAck("table", 1L))
    }

    "count a multiple items in a collection" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Upsert("table", "key1", Payload1)
      expectMsg(UpsertAck("table", "key1"))
      ab ! Count("table")
      expectMsg(CountAck("table", 2L))
    }

    "send an error in case of count to an inexistent collection" in {
      ab ! Count("table1")
      expectMsg(CountNAck("table1", "Collection table1 does not exist"))
    }

    "not count upserts that receives a nack" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Upsert("table", null, Payload1)
      expectMsg(UpsertNAck("table", null, "Key cannot be null"))
      ab ! Count("table")
      expectMsg(CountAck("table", 1L))
    }

    "get a previous upserted item in an empty table" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Find("table", "key")
      expectMsg(FindAck("table", "key", Option(Payload)))
    }

    "get a none for a key not present" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Find("table", "key1")
      expectMsg(FindAck("table", "key1", None))
    }

    "send an error in case of find to an inexistent collection" in {
      ab ! Find("table1", "key")
      expectMsg(FindNAck("table1", "key", "Collection table1 does not exist"))
    }

    "get a previous upserted item in a table containing more than one item" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab! Upsert("table" ,"key1", Payload1)
      expectMsg(UpsertAck("table" ,"key1"))
      ab ! Find("table" ,"key")
      expectMsg(FindAck("table", "key", Option(Payload)))
    }

    "send an error in case of deletion of an inexistent collection" in {
      ab ! Delete("table1", "key")
      expectMsg(DeleteNAck("table1", "key", "Collection table1 does not exist"))
    }

    "delete a previously inserted key" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Delete("table", "key")
      expectMsg(DeleteAck("table", "key"))
      ab ! Find("table", "key")
      expectMsg(FindAck("table", "key", None))
      ab ! Count("table")
      expectMsg(CountAck("table", 0))
    }

    /*
    // FIXME
    "manage to delete a key that does not exist" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Delete("table", "key1")
      expectMsg(DeleteAck("table", "key1"))
      ab ! Find("table", "key")
      expectMsg(FindAck("table", "key", Some(Payload)))
      ab ! Count("table")
      expectMsg(CountAck("table", 1))
    }
    */

    "delete a previously inserted key (more than one key present)" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Upsert("table", "key1", Payload1)
      expectMsg(UpsertAck("table", "key1"))
      ab ! Delete("table", "key")
      expectMsg(DeleteAck("table", "key"))
      ab ! Find("table", "key")
      expectMsg(FindAck("table", "key", None))
      ab ! Find("table", "key1")
      expectMsg(FindAck("table", "key1", Some(Payload1)))
      ab ! Count("table")
      expectMsg(CountAck("table", 1))
    }

    "deletion a previously inserted key and then insert another key" in {
      ab ! Upsert("table", "key", Payload)
      expectMsg(UpsertAck("table", "key"))
      ab ! Delete("table", "key")
      expectMsg(DeleteAck("table", "key"))
      ab ! Upsert("table", "key1", Payload1)
      expectMsg(UpsertAck("table", "key1"))
      ab ! Find("table", "key")
      expectMsg(FindAck("table", "key", None))
      ab ! Find("table", "key1")
      expectMsg(FindAck("table", "key1", Some(Payload1)))
      ab ! Count("table")
      expectMsg(CountAck("table", 1))
    }
  }

  private def createCollection(name: String) = {
    ab ! CreateCollection(name)
    expectMsg(CreateCollectionAck(name))
  }
}