package io.actorbase.actor.main

import akka.actor.{Actor, ActorRef, Props}
import io.actorbase.actor.main.Actorbase.Request.CreateCollection
import io.actorbase.actor.main.Actorbase.Response.{CreateCollectionAck, CreateCollectionNAck}
import io.actorbase.actor.storefinder.StoreFinder

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
  * Access to actorbase from the outside
  *
  * @author Riccardo Cardin
  * @version 1.0
  * @since 1.0
  */
class Actorbase extends Actor {

  override def receive = emptyDatabase

  def emptyDatabase: Receive = {
    case CreateCollection(name) =>
      try {
        val table = context.actorOf(Props(new StoreFinder(name)))
        sender() ! CreateCollectionAck(name)
        context.become(nonEmptyDatabase(Map(name -> table)))
      } catch {
        case ex: Exception =>
          sender() ! CreateCollectionNAck(name, ex.getMessage)
      }
  }

  def nonEmptyDatabase(tables: Map[String, ActorRef]): Receive = ???
}

object Actorbase {

  sealed trait Message

  // Request messages
  object Request {
    case class CreateCollection(name: String) extends Message
  }
  // Response messages
  object Response {
    case class CreateCollectionAck(name: String) extends Message
    case class CreateCollectionNAck(name: String, error: String) extends Message
  }
}
