package io.actorbase.actor.storefinder

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, Router, SmallestMailboxRoutingLogic}
import io.actorbase.actor.storefinder.StoreFinder.Request.{Count, Delete, Query, Upsert}
import io.actorbase.actor.storefinder.StoreFinder.Response._
import io.actorbase.actor.storefinder.StoreFinder.{Message, NumberOfPartitions}
import io.actorbase.actor.storekeeper.Storekeeper
import io.actorbase.actor.storekeeper.Storekeeper.Request
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Get, Put, Remove}
import io.actorbase.actor.storekeeper.Storekeeper.Response._

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
  * Represents a named partitioned map.
  *
  * FIXME Refactor
  *
  * @author Riccardo Cardin
  * @version 0.1
  * @since 0.1
  */
class StoreFinder(val name: String) extends Actor {

  // Partitions of the table
  private val partitions =
    Vector.fill(NumberOfPartitions) {
      context.actorOf(Props[Storekeeper], name = s"$name${UUID.randomUUID()}")
    }

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

  override def receive: Receive = {
    case Upsert(key, payload, u) =>
      val originalSender = sender()
      val handler = context.actorOf(Props(new UpsertResponseHandler(originalSender)))
      upsertRouter.route(Put(key, payload, u), handler)
    case Query(key, u) =>
      val originalSender = sender()
      val handler = context.actorOf(Props(new QueryResponseHandler(originalSender, NumberOfPartitions)))
      broadcastRouter.route(Get(key, u), handler)
    case Delete(key, u) =>
      val originalSender = sender()
      val handler = context.actorOf(Props(new DeleteResponseHandler(originalSender, NumberOfPartitions)))
      broadcastRouter.route(Remove(key, u), handler)
    case Count(u) =>
      val originalSender = sender()
      val handler = context.actorOf(Props(new CountResponseHandler(originalSender, NumberOfPartitions)))
      broadcastRouter.route(Request.Count(u), handler)
  }
}

object StoreFinder {

  val NumberOfPartitions = 5

  sealed trait Message

  // Incoming messages
  object Request {

    case class Upsert(key: String, byte: Array[Byte], uuid: Long) extends Message

    case class Query(key: String, uuid: Long) extends Message

    case class Delete(key: String, uuid: Long) extends Message

    case class Count(uuid: Long) extends Message
  }

  // Out coming messages
  object Response {
    /**
      * Positive response for the upsert request relative to {{key}}.
      * @param k The key just upserted
      */
    case class UpsertAck(k: String, uuid: Long) extends Message

    case class UpsertNAck(k: String, msg: String, uuid: Long) extends Message

    case class QueryAck(key: String, value: Option[Array[Byte]], uuid: Long) extends Message

    case class CountAck(s: Long, uuid: Long) extends Message

    case class DeleteAck(key: String, uuid: Long) extends Message
  }
}

abstract class Handler(originalSender: ActorRef) extends Actor with ActorLogging {
  def sendResponseAndShutdown(response: Message): Unit = {
    originalSender ! response
    log.debug(s"Stopping context capturing actor $self")
    context.stop(self)
  }
}

/**
  * Handles responses to `Put` messages from `StoreKeeper` actor
  * @param originalSender Original sender of the upsert request
  */
class UpsertResponseHandler(originalSender: ActorRef) extends Handler(originalSender) {
  override def receive: Receive = LoggingReceive {
    case PutAck(key, u) =>
      sendResponseAndShutdown(UpsertAck(key, u))
    case PutNAck(key, msg, u) =>
      sendResponseAndShutdown(UpsertNAck(key, msg, u))
  }
}

class QueryResponseHandler(originalSender: ActorRef, partitions: Int) extends Handler(originalSender) {

  // FIXME: avoid mutable state
  var responses: List[Option[(Array[Byte], Long)]] = Nil

  override def receive: Receive = LoggingReceive {
    case Item(key, opt, u) =>
      responses = opt :: responses
      if (responses.length == partitions) {
        val item = responses.collect {
          case Some(tuple) => tuple
        }.sortBy(_._2)
          .headOption
          .map(_._1)
        sendResponseAndShutdown(QueryAck(key, item, u))
      }
  }
}

class DeleteResponseHandler(originalSender: ActorRef, partitions: Int) extends Handler(originalSender) {

  // FIXME: avoid mutable state
  var numberOfResponses = 0

  override def receive: Receive = LoggingReceive {
    case RemoveAck(key, id) =>
      numberOfResponses += 1
      if (numberOfResponses == NumberOfPartitions) {
        sendResponseAndShutdown(DeleteAck(key, id))
      }
  }
}

class CountResponseHandler(originalSender: ActorRef, partitions: Int) extends Handler(originalSender) {

  var count = 0L
  var numberOfResponses = 0

  override def receive: Receive = LoggingReceive {
    case Size(size, u) =>
      count += size
      numberOfResponses += 1
      if (numberOfResponses == NumberOfPartitions) {
        sendResponseAndShutdown(CountAck(count, u))
      }
  }
}
