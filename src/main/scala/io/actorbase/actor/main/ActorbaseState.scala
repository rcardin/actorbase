package io.actorbase.actor.main

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
  * Traces the pending requests from and to a Actorbase.
  *
  * @author Riccardo Cardin
  * @version 0.1
  * @since 0.1
  */
protected case class ActorbaseState(upserts: Map[Long, ActorbaseRequest],
                                    queries: Map[Long, ActorbaseRequest],
                                    deletions: Map[Long, ActorbaseRequest],
                                    counts: Map[Long, ActorbaseRequest]) {
  def addUpsert(uuid: Long, request: ActorbaseRequest): ActorbaseState = {
    copy(upserts = upserts + (uuid -> request))
  }
  def removeUpsert(uuid: Long): (Option[ActorbaseRequest], ActorbaseState) = {
    (upserts.get(uuid), copy(upserts = upserts - uuid))
  }
  def addQuery(uuid: Long, request: ActorbaseRequest): ActorbaseState = {
    copy(queries = queries + (uuid -> request))
  }
  def removeQuery(uuid: Long): (Option[ActorbaseRequest], ActorbaseState) = {
    (queries.get(uuid), copy(queries = queries - uuid))
  }
  def addDeletion(uuid: Long, request: ActorbaseRequest): ActorbaseState = {
    copy(deletions = deletions + (uuid -> request))
  }
  def removeDeletion(uuid: Long): (Option[ActorbaseRequest], ActorbaseState) = {
    (deletions.get(uuid), copy(deletions = deletions - uuid))
  }
  def addCount(uuid: Long, request: ActorbaseRequest): ActorbaseState = {
    copy(counts = counts + (uuid -> request))
  }
  def removeCount(uuid: Long): (Option[ActorbaseRequest], ActorbaseState) = {
    (counts.get(uuid), copy(counts = counts - uuid))
  }
}

object ActorbaseState {
  def apply(): ActorbaseState = new ActorbaseState(Map(), Map(), Map(), Map())
}
