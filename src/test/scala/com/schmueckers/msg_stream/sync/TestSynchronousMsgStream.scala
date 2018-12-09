package com.schmueckers.msg_stream.sync

import com.schmueckers.msg_stream.{MsgStream, MsgStreamTestTemplate}

class TestSynchronousMsgStream extends MsgStreamTestTemplate {
  override def newStream: MsgStream[String, String] = SynchronousStream[String]
}