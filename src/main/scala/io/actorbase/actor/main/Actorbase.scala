
package io.actorbase.actor.main

import akka.actor.{Actor, ActorRef, Props}
import io.actorbase.actor.main.Actorbase.Request.{CreateCollection, Find, Upsert}
import io.actorbase.actor.main.Actorbase.Response.{CreateCollectionAck, CreateCollectionNAck, FindAck, UpsertNAck}
import io.actorbase.actor.storefinder.StoreFinder
import io.actorbase.actor.storefinder.StoreFinder.Request
import io.actorbase.actor.storefinder.StoreFinder.Request.Query
import io.actorbase.actor.storefinder.StoreFinder.Response.QueryAck

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
  * Access to actorbase from the outside
  *
  * @author Riccardo Cardin
  * @version 1.0
  * @since 1.0
  */
class Actorbase extends Actor {

  override def receive = emptyDatabase

  def emptyDatabase: Receive = {
    case CreateCollection(name) => createCollection(name)
    case Find(collection, id) => sender() ! FindAck(collection, id, None)
    case Upsert(collection, id, value) =>
      replyInsertOnNotExistingCollection(collection, id)
  }

  def nonEmptyDatabase(tables: Map[String, ActorRef], state: ActorbaseState): Receive = {
    case CreateCollection(name) =>
      if (!tables.isDefinedAt(name)) createCollection(name)
      else sender() ! CreateCollectionNAck(name, s"Collection $name already exists")
    case Find(collection, id) =>
      tables.get(id) match {
        case Some(finder) =>
          val u = uuid()
          finder ! Query(id, u)
          context.become(nonEmptyDatabase(tables, state.addQuery(u, sender())))
        case None => sender() ! FindAck(collection, id, None)
      }
    case QueryAck(key, value, u) =>
      // FIXME I lost the collection in this way :(
      state.queries.get(u)foreach(actor => actor ! FindAck("", key, value))
    case Upsert(collection, id, value) =>
      tables.get(id) match {
        case Some(finder) =>
          val u = uuid()
          finder ! Request.Upsert(id, value, u)
          context.become(nonEmptyDatabase(tables, state.addUpsert(u, sender())))
        case None => replyInsertOnNotExistingCollection(collection, id)
      }
    // TODO Manage upsert responses
  }

  private def replyInsertOnNotExistingCollection(collection: String, id: String) = {
    sender() ! UpsertNAck(collection, id, s"Collection $collection does not exist")
  }

  private def createCollection(name: String) = {
    try {
      val table = context.actorOf(Props(new StoreFinder(name)))
      sender() ! CreateCollectionAck(name)
      context.become(nonEmptyDatabase(Map(name -> table), ActorbaseState()))
    } catch {
      case ex: Exception =>
        sender() ! CreateCollectionNAck(name, ex.getMessage)
    }
  }

  private def uuid(): Long = System.currentTimeMillis()
}

object Actorbase {

  sealed trait Message

  // Request messages
  object Request {
    case class CreateCollection(name: String) extends Message
    case class Find(collection: String, id: String) extends Message
    case class Upsert(collection: String, id: String, value: Array[Byte]) extends
  }
  // Response messages
  object Response {
    case class CreateCollectionAck(name: String) extends Message
    case class CreateCollectionNAck(name: String, error: String) extends Message
    case class FindAck(collection: String, id: String, value: Option[Array[Byte]])
    case class FindNAck(collection: String, id: String, error: String)
    case class UpsertAck(collection: String, id: String)
    case class UpsertNAck(collection: String, id: String, error: String)
  }
}
