package io.actorbase.actor.storefinder

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.{ActorRefRoutee, BroadcastRoutingLogic, Router, SmallestMailboxRoutingLogic}
import io.actorbase.actor.storefinder.StoreFinder.NumberOfPartitions
import io.actorbase.actor.storefinder.StoreFinder.Request.{Count, Delete, Query, Upsert}
import io.actorbase.actor.storefinder.StoreFinder.Response._
import io.actorbase.actor.storekeeper.Storekeeper
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Get, Put, Remove}
import io.actorbase.actor.storekeeper.Storekeeper.Response.{Item, PutAck, PutNAck, RemoveAck}

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
  * Represents a named partitioned map.
  *
  * FIXME Refactor
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
    case Upsert(key, payload, u) =>
      upsertRouter.route(Put(key, payload, u), self)
      val initialState = StoreFinderState()
      context.become(almostEmptyTable(initialState.addUpsert(u, sender())))
    case Query(key, u) => sender ! QueryAck(key, None, u)
    case Count(u) => sender ! CountAck(0, u)
    case Delete(key, u) => sender ! DeleteAck(key, u)
  }

  def almostEmptyTable(state: StoreFinderState): Receive = {
    case Upsert(key, payload, u) =>
      upsertRouter.route(Put(key, payload, u), self)
      context.become(almostEmptyTable(state.addUpsert(u, sender())))
    case Query(key, u) => sender ! QueryAck(key, None, u)
    case Count(u) => sender ! CountAck(0, u)
    case Delete(key, u) => sender ! DeleteAck(key, u)
    case PutAck(key, u) =>
      state.upserts.get(u).collect {
        case senderActor =>
          senderActor ! UpsertAck(key, u)
          context.become(nonEmptyTable(state.upsertAck(u)))
      }
    case PutNAck(key, msg, u) =>
      state.upserts.get(u).collect {
        case senderActor =>
          senderActor ! UpsertNAck(key, msg, u)
          if (state.upserts.size == 1)
            context.become(emptyTable())
          else
            context.become(almostEmptyTable(state.upsertNAck(u)))
      }
  }

  def nonEmptyTable(state: StoreFinderState): Receive = {
    case Upsert(key, payload, u) =>
      // FIXME There is a problem with upsert: We don't know where is the previous value
      upsertRouter.route(Put(key, payload, u), self)
      context.become(nonEmptyTable(state.addUpsert(u, sender())))
    case Query(key, u) =>
      broadcastRouter.route(Get(key, u), self)
      // FIXME Improve syntax
      context.become(nonEmptyTable(state.addQuery(key, u, sender())))
    case Count(u) => sender ! CountAck(state.count, u)
    case Delete(key, u) =>
      broadcastRouter.route(Remove(key, u), self)
      context.become(nonEmptyTable(state.addErasure(u, sender())))
    // FIXME This code is repeated
    case PutAck(key, u) =>
      state.upserts.get(u).collect {
        case senderActor =>
          senderActor ! UpsertAck(key, u)
          context.become(nonEmptyTable(state.upsertAck(u)))
      }
    case PutNAck(key, msg, u) =>
      state.upserts.get(u).collect {
        case senderActor =>
          senderActor ! UpsertNAck(key, msg, u)
          context.become(nonEmptyTable(state.upsertNAck(u)))
      }
    case res: Item =>
      context.become(nonEmptyTable(state.copy(queries = item(res, state.queries))))
    case rm: RemoveAck =>
      val newPendingErasures = delete(rm, state.erasures)
      if (state.erasures.size > newPendingErasures.size) {
        // FIXME By now, we do not return to empty table: implement a procedure that
        //       fixes any pending request on a table that became empty
        context.become(nonEmptyTable(state.copy(erasures = newPendingErasures, count = state.count - 1)))
      } else {
        context.become(nonEmptyTable(state.copy(erasures = newPendingErasures)))
      }
  }

  private def item(response: Item,
                   queries: Map[Long, QueryReq]): Map[Long, QueryReq] = {
    val Item(key, opt, id) = response
    val QueryReq(actor, responses) = queries.get(id).get
    val newResponses = opt :: responses
    if (newResponses.length == NumberOfPartitions) {
      val item = newResponses.collect {
        case Some(tuple) => tuple
      }.sortBy(_._2)
        .headOption
        .map(_._1)
      actor ! QueryAck(key, item, id)
      queries - id
    } else {
      queries + (id -> QueryReq(actor, newResponses))
    }
  }

  private def delete(remove: RemoveAck, erasures: Map[/*uuid*/ Long, (Int, ActorRef)]): Map[Long, (Int, ActorRef)] = {
    val RemoveAck(key, id) = remove
    val (responses, actor) = erasures.get(id).get
    val newResponses = responses + 1
    if (newResponses == NumberOfPartitions) {
      actor ! DeleteAck(key, id)
      erasures - id
    } else {
      erasures + (id -> (newResponses, actor))
    }
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

    case class DeleteAck(key: String, uuid: Long)
  }
}
