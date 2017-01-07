package io.actorbase.actor.storefinder

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, Router, SmallestMailboxRoutingLogic}
import io.actorbase.actor.storefinder.StoreFinder.NumberOfPartitions
import io.actorbase.actor.storefinder.StoreFinder.Request.{Count, Delete, Query, Upsert}
import io.actorbase.actor.storefinder.StoreFinder.Response._
import io.actorbase.actor.storekeeper.Storekeeper
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Get, Put}
import io.actorbase.actor.storekeeper.Storekeeper.Response.{Item, PutAck, PutNAck}

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

  // TODO What if we count here elements of the table?

  override def receive: Receive = emptyTable()

  def emptyTable(): Receive = {
    // External interface
    case Upsert(key, payload) =>
      val id = uuid()
      upsertRouter.route(Put(key, payload, id), self)
      context.become(almostEmptyTable(Map(id -> sender())))
    case Query(key) => sender ! QueryAck(key, None)
    case Count => sender ! CountAck(0)
    case Delete(key) => sender ! DeleteAck(key)
  }

  def almostEmptyTable(pendingUpserts: Map[/*uuid*/Long, ActorRef]): Receive = {
    case Upsert(key, payload) =>
      val u = uuid()
      upsertRouter.route(Put(key, payload, u), self)
      context.become(almostEmptyTable(pendingUpserts + (u -> sender())))
    case Query(key) => sender ! QueryAck(key, None)
    case Count => sender ! CountAck(0)
    case Delete(key) => sender ! DeleteAck(key)
    case PutAck(key, u) =>
      pendingUpserts.get(u).collect {
        case senderActor =>
          senderActor ! UpsertAck(key)
          context.become(nonEmptyTable(pendingUpserts - u, Map(), 1))
      }
    case PutNAck(key, msg, id) =>
      pendingUpserts.get(id).collect {
        case senderActor =>
          senderActor ! UpsertNAck(key, msg)
          if (pendingUpserts.size == 1)
            context.become(emptyTable())
          else
            context.become(almostEmptyTable(pendingUpserts - id))
      }
  }

  def nonEmptyTable(pendingUpserts: Map[/*uuid*/ Long, ActorRef],
                    pendingQueries: Map[/*uuid*/ Long, QueryReq],
                    count: Long): Receive = {
    case Upsert(key, payload) =>
      // FIXME There is a problem with upsert: We don't know where is the previous value
      val id = uuid()
      upsertRouter.route(Put(key, payload, id), self)
      context.become(nonEmptyTable(pendingUpserts + (id -> sender()), pendingQueries, count))
    case Query(key) =>
      val id = uuid()
      broadcastRouter.route(Get(key, id), self)
      // FIXME Improve syntax
      context.become(nonEmptyTable(pendingUpserts, pendingQueries + (id -> QueryReq(sender(), Nil)), count))
    // FIXME This code is repeated
    case PutAck(key, u) =>
      pendingUpserts.get(u).collect {
        case senderActor =>
          senderActor ! UpsertAck(key)
          context.become(nonEmptyTable(pendingUpserts - u, pendingQueries, count + 1))
      }
    case PutNAck(key, msg, id) =>
      pendingUpserts.get(id).collect {
        case senderActor =>
          senderActor ! UpsertNAck(key, msg)
          context.become(nonEmptyTable(pendingUpserts - id, pendingQueries, count))
      }
    case res: Item =>
      context.become(nonEmptyTable(pendingUpserts, item(res, pendingQueries), count))
  }

  private def item(response: Item,
                   queries: Map[Long, QueryReq]): Map[Long, QueryReq] = {
    val Item(key, opt, id) = response
    queries.get(id).collect {
      case QueryReq(actor, responses) =>
        val newResponses = opt :: responses
        if (newResponses.length == NumberOfPartitions) {
          val item = newResponses.collect {
            case Some(tuple) => tuple
          }.sortBy(_._2)
            .headOption
            .map(_._1)
          actor ! QueryAck(key, item)
          queries - id
        } else {
          queries + (id -> QueryReq(actor, newResponses))
        }
    }
    queries
  }

  private def uuid(): Long = System.currentTimeMillis()
}

/**
  * TODO
  * @param sender
  * @param responses
  */
case class QueryReq(sender: ActorRef,
                    responses: List[Option[(Array[Byte], Long)]])

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

    case class UpsertNAck(k: String, msg: String) extends Message

    case class QueryAck(key: String, value: Option[Array[Byte]]) extends Message

    case class CountAck(s: Long) extends Message

    case class DeleteAck(key: String)
  }
}
