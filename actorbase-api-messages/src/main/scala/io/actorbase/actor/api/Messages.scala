package io.actorbase.actor.api

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
  * External API of Actobase system
  */
object Messages {
  // Request messages
  object Request {

    case class CreateCollection(name: String)

    case class Find(collection: String, id: String)

    case class Upsert(collection: String, id: String, value: Array[Byte])

    case class Delete(collection: String, id: String)

    case class Count(collection: String)
  }
  // Response messages
  object Response {

    case class CreateCollectionAck(name: String)

    case class CreateCollectionNAck(name: String, error: String)

    case class FindAck(collection: String, id: String, value: Option[Array[Byte]])

    case class FindNAck(collection: String, id: String, error: String)

    case class UpsertAck(collection: String, id: String)

    case class UpsertNAck(collection: String, id: String, error: String)

    case class DeleteAck(collection: String, id: String)

    case class DeleteNAck(collection: String, id: String, error: String)

    case class CountAck(collection: String, count: Long)

    case class CountNAck(collection: String, error: String)
  }
}
