package io.actorbase.actor.storekeeper

import java.util.Objects

import akka.actor.Actor
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Count, Get, Put, Remove}
import io.actorbase.actor.storekeeper.Storekeeper.Response._

import scala.util.{Failure, Success, Try}

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
  * Stores couples of {{String}} -> {{Array[Byte]}}.
  *
  * @author Riccardo Cardin
  * @version 1.0
  * @since 1.0
  */
class Storekeeper extends Actor {
  override def receive = emptyMap

  def emptyMap: Receive = {
    case Put(k, v) => put(Map[String, Array[Byte]](), k, v)
    case Get(k) => sender ! Item(k, None)
    case Count => sender ! Size(0L)
    case Remove(k) => sender ! RemoveAck(k)
  }

  def nonEmptyMap(store: Map[String, Array[Byte]]): Receive = {
    case Put(k, v) => put(store, k, v)
    case Get(k) => sender ! Item(k, store.get(k))
    case Remove(k) => remove(store, k)
    case Count => sender ! Size(store.size)
  }

  private def put(store: Map[String, Array[Byte]], key: String, value: Array[Byte]) = {
    try {
      if (key != null) {
        val newStore = store + (Objects.requireNonNull(key) -> value)
        sender ! PutAck(key)
        context.become(nonEmptyMap(newStore))
      } else {
        sender ! PutNAck(Storekeeper.KeyNull)
      }
    } catch {
      case ex: Exception =>
        sender ! PutNAck(ex.getMessage)
    }
  }

  private def remove(store: Map[String, Array[Byte]], key: String) = {
    val newStore = store - key
    sender ! RemoveAck(key)
    context.become {
      if (newStore.isEmpty) emptyMap
      else nonEmptyMap(newStore)
    }
  }
}

object Storekeeper {

  val KeyNull = "Key cannot be null"

  sealed trait Message

  // Input messages
  object Request {

    /**
      * Upsert operation which eventually updates the previous value of {{key}}.
      * The key must be not null.
      * @param key  A key
      * @param byte A value
      */
    case class Put(key: String, byte: Array[Byte]) extends Message

    /**
      * Request for the value associated to {{key}}.
      * @param key A key
      */
    case class Get(key: String) extends Message

    /**
      * Request for the deletion of the value associated to {{key}}.
      * @param key A key
      */
    case class Remove(key: String) extends Message

    /**
      * Request for the actual size of the map.
      */
    case object Count extends Message
  }

  // Output messages
  object Response {

    /**
      * Positive response for the upsert request relative to {{key}}.
      * @param k The key just upserted
      */
    case class PutAck(k: String) extends Message

    /**
      * Negative response for the upsert request relative to {{key}}.
      * @param msg Error message received during upsertion
      */
    case class PutNAck(msg: String) extends Message

    case class Item(key: String, value: Option[Array[Byte]]) extends Message

    case class Size(s: Long) extends Message

    case class RemoveAck(key: String)
  }
}
