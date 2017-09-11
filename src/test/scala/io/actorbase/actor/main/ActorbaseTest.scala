package io.actorbase.actor.main

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.actorbase.actor.main.Actorbase.Request._
import io.actorbase.actor.main.Actorbase.Response._
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
  */

/**
  * Tests relative to Actorbase (main) actor.
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

  "An non empty database" must {
    "send an error if it tries to create the same collection more than once" in {
      ab ! CreateCollection("table")
      expectMsg(CreateCollectionAck("table"))
      ab ! CreateCollection("table")
      expectMsg(CreateCollectionNAck("table", "Collection table already exists"))
    }
  }
}
