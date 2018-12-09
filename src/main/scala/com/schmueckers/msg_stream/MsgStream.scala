package com.schmueckers.msg_stream

/**
  * A Sink you can publish to
  *
  * @tparam I
  */
trait MsgSink[I] {
  def !(i: I)

  def ::[II](head: MsgStream[II, I]): MsgSink[II] = head.foreach( x => this ! x )
  def ::[I1,I2](hs: (MsgStream[I1, I], MsgStream[I2, I])): (MsgSink[I1],MsgSink[I2]) =
    ( hs._1 :: this, hs._2 :: this)
}

trait MsgStreamHeader[I] extends MsgStream[I,I]

/**
  * A Stream which offers flatMap and foreach to turn it into a Sink
  *
  * @tparam I
  * @tparam O
  */
trait MsgStream[I, O] {
  def ::[II](head: MsgStream[II, I]): MsgStream[II,O]
  def ::[I1,I2](heads: (MsgStream[I1, I], MsgStream[I2,I]) ): (MsgStream[I1,O],MsgStream[I2,O]) =
    ( heads._1 :: this, heads._2 :: this)

  def foreach(p: O => Unit): MsgSink[I]

  def flatMap[R](f: O => Seq[R]): MsgStream[I, R]

  def map[R](f: O => R): MsgStream[I, R] = flatMap(o => List(f(o)))

  def foldByKey[K, S](keyFkt: O => K, seed: S)(agg: PartialFunction[(K, S, O), S]):
  MsgStream[I, (K, S)] = foldByKey[K, S](keyFkt, (k: K) => seed)(agg)

  def foldByKey[K, S](keyFkt: O => K, seed: (K) => S)(agg: PartialFunction[(K, S, O), S]):
  MsgStream[I, (K, S)]

  def fork(sinks: MsgSink[O]*): MsgSink[I] =
    foreach(x => {
      sinks.foreach(_ ! x)
    })
}

