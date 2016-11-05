package io.actorbase.actor.storekeeper

import akka.actor.Actor
import io.actorbase.actor.storekeeper.Storekeeper.Request.{Count, Get, Put, Remove}
import io.actorbase.actor.storekeeper.Storekeeper.Response.{Item, Size}

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
    case Put(k, v) =>
      context.become(nonEmptyMap(Map[String, Array[Byte]](k -> v)))
    case Count => sender ! Size(0L)
  }

  def nonEmptyMap(store: Map[String, Array[Byte]]): Receive = {
    case Put(k, v) => context.become(nonEmptyMap(store + (k -> v)))
    case Get(k) => sender ! Item(k, store.get(k))
    case Remove(k) =>
      val newStore = store - k
      context.become {
        if (newStore.isEmpty) emptyMap
        else nonEmptyMap(newStore)
      }
    case Count => sender ! Size(store.size)
  }
}

object Storekeeper {
  sealed trait Message

  // Input messages
  object Request {
    case class Put(key: String, byte: Array[Byte]) extends Message

    case class Get(key: String) extends Message

    case class Remove(key: String) extends Message

    case object Count extends Message

  }

  // Output messages
  object Response {

    case class Item(key: String, value: Option[Array[Byte]]) extends Message

    case class Size(s: Long) extends Message

  }
}
