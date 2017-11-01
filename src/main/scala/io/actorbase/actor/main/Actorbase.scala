
package io.actorbase.actor.main

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import io.actorbase.actor.api.Api.Request._
import io.actorbase.actor.api.Api.Response._
import io.actorbase.actor.main.Actorbase.uuid
import io.actorbase.actor.storefinder.StoreFinder
import io.actorbase.actor.storefinder.StoreFinder.Request.Query
import io.actorbase.actor.storefinder.StoreFinder.Response.QueryAck
import io.actorbase.actor.storefinder.StoreFinder.{Request, Response}

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
  * Access to actorbase from the outside
  *
  * @author Riccardo Cardin
  * @version 0.1
  * @since 0.1
  */
class Actorbase extends Actor {

  var tables: scala.collection.mutable.Map[String, Collection] = scala.collection.mutable.Map()

  override def receive: Receive = {
    manageCreations(tables)
      .orElse(manageQueries(tables))
      .orElse(manageUpserts(tables))
      .orElse(manageDeletions(tables))
      .orElse(manageCounts(tables))
  }

  private def manageCreations(tables: scala.collection.mutable.Map[String, Collection]): Receive = {
    case CreateCollection(name) =>
      if (!tables.isDefinedAt(name)) createCollection(name)
      else sender() ! CreateCollectionNAck(name, s"Collection $name already exists")
  }

  private def manageUpserts(tables: scala.collection.mutable.Map[String, Collection]): Receive = {
    case Upsert(coll, id, value) =>
      tables.get(coll) match {
        case Some(Collection(name, finder)) =>
          val u = uuid()
          val originalSender = sender()
          val handler = context.actorOf(Props(new UpsertResponseHandler(name, originalSender)))
          finder.tell(Request.Upsert(id, value, u), handler)
        case None => replyInsertOnNotExistingCollection(coll, id)
      }
  }

  private def manageQueries(tables: scala.collection.mutable.Map[String, Collection]): Receive = {
    case Find(coll, id) =>
      tables.get(coll) match {
        case Some(Collection(name, finder)) =>
          val u = uuid()
          val originalSender = sender()
          val handler = context.actorOf(Props(new QueryResponseHandler(name, originalSender)))
          finder.tell(Query(id, u), handler)
        case None => replyFindOnNotExistingCollection(coll, id)
      }
  }

  private def manageDeletions(tables: scala.collection.mutable.Map[String, Collection]): Receive = {
    case Delete(coll, id) =>
      tables.get(coll) match {
        case Some(Collection(name, finder)) =>
          val u = uuid()
          val originalSender = sender()
          val handler = context.actorOf(Props(new DeleteResponseHandler(name, originalSender)))
          finder.tell(Request.Delete(id, u), handler)
        case None => replyDeleteOnNotExistingCollection(coll, id)
      }
  }

  def manageCounts(tables: scala.collection.mutable.Map[String, Collection]): Receive = {
    case Count(coll) =>
      tables.get(coll) match {
        case Some(Collection(name, finder)) =>
          val u = uuid()
          val originalSender = sender()
          val handler = context.actorOf(Props(new CountResponseHandler(name, originalSender)))
          finder.tell(Request.Count(u), handler)
        case None => replyCountOnNotExistingCollection(coll)
      }
  }

  private def replyFindOnNotExistingCollection(collection: String, id: String): Unit = {
    sender() ! FindNAck(collection, id, s"Collection $collection does not exist")
  }

  private def replyInsertOnNotExistingCollection(collection: String, id: String): Unit = {
    sender() ! UpsertNAck(collection, id, s"Collection $collection does not exist")
  }

  private def replyDeleteOnNotExistingCollection(collection: String, id: String): Unit = {
    sender() ! DeleteNAck(collection, id, s"Collection $collection does not exist")
  }

  private def replyCountOnNotExistingCollection(collection: String): Unit = {
    sender() ! CountNAck(collection, s"Collection $collection does not exist")
  }

  private def createCollection(name: String): Unit = {
    try {
      val table = context.actorOf(Props(new StoreFinder(name)))
      sender() ! CreateCollectionAck(name)
      tables.+=((name, Collection(name, table)))
    } catch {
      case ex: Exception =>
        sender() ! CreateCollectionNAck(name, ex.getMessage)
    }
  }
}

object Actorbase {
  private def uuid(): Long = System.currentTimeMillis()
}

// TODO Is it possible to make this a trait to share among cameo actors?
abstract class Handler(originalSender: ActorRef) extends Actor with ActorLogging {
  def sendResponseAndShutdown(response: Any): Unit = {
    originalSender ! response
    log.debug(s"Stopping context capturing actor $self")
    context.stop(self)
  }
}

/**
  * Handles responses to `Upsert` messages from `Actorbase` actor
  * @param originalSender Original sender of the upsert request
  */
class UpsertResponseHandler(collection: String, originalSender: ActorRef) extends Handler(originalSender) {
  override def receive: Receive = LoggingReceive {
    case Response.UpsertNAck(key, msg, u) =>
      sendResponseAndShutdown(UpsertNAck(collection, key, msg))
    case Response.UpsertAck(key, u) =>
      sendResponseAndShutdown(UpsertAck(collection, key))
  }
}

class QueryResponseHandler(collection: String, originalSender: ActorRef) extends Handler(originalSender) {
  override def receive: Receive = LoggingReceive {
    case QueryAck(key, value, u) =>
      sendResponseAndShutdown(FindAck(collection, key, value))
  }
}

class DeleteResponseHandler(collection: String, originalSender: ActorRef) extends Handler(originalSender) {
  override def receive: Receive = LoggingReceive {
    case Response.DeleteAck(key, u) =>
      sendResponseAndShutdown(DeleteAck(collection, key))
  }
}

class CountResponseHandler(collection: String, originalSender: ActorRef) extends Handler(originalSender) {
  override def receive: Receive = LoggingReceive {
    case Response.CountAck(count, u) =>
      sendResponseAndShutdown(CountAck(collection, count))
  }
}