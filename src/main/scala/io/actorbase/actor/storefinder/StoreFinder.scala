package io.actorbase.actor.storefinder

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, Router, SmallestMailboxRoutingLogic}
import io.actorbase.actor.storefinder.StoreFinder.NumberOfPartitions
import io.actorbase.actor.storefinder.StoreFinder.Request.{Count, Delete, Query, Upsert}
import io.actorbase.actor.storefinder.StoreFinder.Response.{CountAck, DeleteAck, QueryAck}
import io.actorbase.actor.storekeeper.Storekeeper
import io.actorbase.actor.storekeeper.Storekeeper.Request.Put

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
  * Represents a named partitioned map.
  *
  * @author Riccardo Cardin
  * @version 1.0
  * @since 1.0
  */
class StoreFinder(val name: String) extends Actor {

  // Partitions of the table
  private val partitions =
    Vector.fill(NumberOfPartitions)
      {context.actorOf(Props[Storekeeper], name = s"$name${UUID.randomUUID()}")}

  // Routers
  // Insertion routing strategy
  private val upsertRouter = {
    val routees = partitions.map(ActorRefRoutee)
    Router(SmallestMailboxRoutingLogic(), routees)
  }
  // All other messages routing strategy
  private val broadcastRouter = {
    val routees = partitions.map(ActorRefRoutee)
    Router(BroadcastRoutingLogic(), routees)
  }

  override def receive: Receive = emptyTable()

  def emptyTable(): Receive = {
    // External interface
    case Upsert(key, payload) => upsertRouter.route(Put(key, payload), sender)
    case Query(key) => sender ! QueryAck(key, None)
    case Count => sender ! CountAck(0)
    case Delete(key) => sender ! DeleteAck(key)
  }

  def nonEmptyTable(index: List[(String, ActorRef)]): Receive = ???
}

object StoreFinder {

  val NumberOfPartitions = 5

  sealed trait Message

  // Incoming messages
  object Request {

    case class Upsert(key: String, byte: Array[Byte]) extends Message

    case class Query(key: String) extends Message

    case class Delete(key: String) extends Message

    case object Count extends Message
  }

  // Out coming messages
  object Response {
    /**
      * Positive response for the upsert request relative to {{key}}.
      * @param k The key just upserted
      */
    case class UpsertAck(k: String) extends Message

    case class QueryAck(key: String, value: Option[Array[Byte]]) extends Message

    case class CountAck(s: Long) extends Message

    case class DeleteAck(key: String)
  }
}
