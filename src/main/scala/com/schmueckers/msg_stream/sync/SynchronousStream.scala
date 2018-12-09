package com.schmueckers.msg_stream.sync

import com.schmueckers.msg_stream.{MsgSink, MsgStream}

object SynchronousStream {
  def apply[I] = new SynchronousStream[I,I] {
    def foreach(p: I => Unit): MsgSink[I] = new MsgSink[I] {
      override def !(i: I) = p(i)
    }


  }
}

trait SynchronousStream[I, O] extends MsgStream[I, O] {
  def flatMap[R](f: O => Seq[R]) = new SynchronousStream[I, R] {
    override def foreach(p: R => Unit): MsgSink[I] =
      SynchronousStream.this.foreach(
        (o: O) => f(o).foreach(p))
  }

  override def ::[II](head: MsgStream[II, I]): MsgStream[II, O] = new SynchronousStream[II,O] {
    override def foreach(p: O => Unit): MsgSink[II] = head :: SynchronousStream.this.foreach( p )
  }

  override def foldByKey[K, S](keyFkt: O => K, seed: K => S)(agg: PartialFunction[(K, S, O), S]):
  MsgStream[I, (K, S)] = new SynchronousStream[I, (K, S)] {
    var stateByKey = Map.empty[K, S]

    override def foreach(p: ((K, S)) => Unit): MsgSink[I] =
      SynchronousStream.this.foreach((o: O) => {
        val k = keyFkt(o)
        val s = stateByKey.getOrElse(k, seed(k))
        val r = agg((k, s, o))
        stateByKey = stateByKey + (k -> r)
        p((k, r))
      })
  }
}