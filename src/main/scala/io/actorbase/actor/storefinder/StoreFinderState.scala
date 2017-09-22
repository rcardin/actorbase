package io.actorbase.actor.storefinder

import akka.actor.ActorRef

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
  * Traces the pending requests from and to a StoreFinder.
  *
  * @author Riccardo Cardin
  * @version 1.0
  * @since 1.0
  */
case class StoreFinderState(upserts: Map[Long, ActorRef],
                            queries: Map[Long, QueryReq],
                            erasures: Map[Long, (Int, ActorRef)],
                            counts: Map[Long, (Long, Long, ActorRef)]) {
  def addQuery(key: String, id: Long, sender: ActorRef): StoreFinderState = {
    copy(queries = queries + (id -> QueryReq(sender, List[Option[(Array[Byte], Long)]]())))
  }

  def addUpsert(id: Long, sender: ActorRef): StoreFinderState = {
    copy(upserts = upserts + (id -> sender))
  }

  def addErasure(id: Long, sender: ActorRef): StoreFinderState = {
    copy(erasures = erasures + (id -> (0, sender)))
  }

  def addErasure(id: Long, sender: ActorRef, size: Long): StoreFinderState = {
    copy(erasures = erasures + (id -> (0, sender)))
  }

  def upsertAck(id: Long): StoreFinderState = {
    copy(upserts = upserts - id)
  }

  def upsertNAck(id: Long): StoreFinderState = {
    copy(upserts = upserts - id)
  }

  def addCount(id: Long, sender: ActorRef): StoreFinderState = {
    copy(counts = counts + (id -> (0, 0, sender)))
  }

  def removeCounts(id: Long): StoreFinderState = {
    copy(counts = counts - id)
  }

  def countAck(id: Long, size: Long): StoreFinderState = {
    val (req, oldSize, sender) = counts(id)
    copy(counts = counts.updated(id, (req + 1, oldSize + size, sender)))
  }
}

object StoreFinderState {
  def apply(): StoreFinderState = new StoreFinderState(Map(), Map(), Map(), Map())
}
/**
  * TODO
  * @param sender Sender actor
  * @param responses Number of responses
  */
sealed case class QueryReq(sender: ActorRef,
                           responses: List[Option[(Array[Byte], Long)]])
