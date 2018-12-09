package com.schmueckers.msg_stream

import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

/**
  * An Abstract Class which can be extended to test a specific
  * {{Stream}} implementation
  */
abstract class MsgStreamTestTemplate extends FunSpec with Matchers with GivenWhenThen {

  def newStream: MsgStream[String, String]

  val msg = "Hello, world!"
  var results: List[Any] = Nil

  def recordResult = (s: Any) => {
    results = s :: results
  }

  // The following is a test that is not specific to a synchronous message stream functionally
  // but the implementation depends on synchrnonous operation. The results would arrive delayed
  // and therefore we would need a timeout if we do this with some asynchronous MsgStream
  describe("a SynchronousMsgStream") {
    it("should call foreach every time a new msg is dropped into the stream") {
      results = Nil
      val s = newStream.foreach(recordResult)

      s ! msg
      results should be(msg :: Nil)

      s ! msg
      results should be(msg :: msg :: Nil)
    }
    it("should return no elements if flatMap returns an empty sequence") {
      results = Nil

      val s = newStream.flatMap((s: String) => List()).foreach(recordResult)
      s ! msg
      results should be(Nil)
      s ! msg
      results should be(Nil)
    }
    it("should return all the results from the map to a seq") {
      val s = newStream.flatMap((s: String) => List(s, s)).foreach(recordResult)

      s ! msg
      results should be(msg :: msg :: Nil)
      s ! msg
      results should be(msg :: msg :: msg :: msg :: Nil)
    }
    it("should allow to chain flatmaps") {
      results = Nil

      def duplicate(s: String) = s :: s :: Nil

      val s = newStream.flatMap(duplicate).flatMap(duplicate).foreach(recordResult)
      s ! msg
      results should be(duplicate(msg) ++ duplicate(msg))
    }
  }
  describe("a MsgStream") {
    val sample = List("a", "b", "a", "c")
    val expectedResult = List(("a", 1), ("b", 1), ("a", 2), ("c", 1))

    it("should count words correctly by keys") {
      results = Nil

      val s: MsgStream[String, (String, Int)] = newStream.foldByKey((s: String) => s, 0) {
        case x: (String, Int, String) => x._2 + 1
      }

      val s2 = s.foreach(recordResult)

      for {msg <- sample} {
        s2 ! msg
      }

      results.reverse should be(expectedResult)
    }
    it("should allow to map and then count by key") {
      results = Nil

      def id(s: String) = s

      val s: MsgStream[String, String] = newStream.map(id)
      val s2 = s.foldByKey((s: String) => s, 0) {
        case x: (String, Int, String) => x._2 + 1
      }

      val s3 = s2.foreach(recordResult)

      for {msg <- sample} {
        s3 ! msg
      }
      results.reverse should be(expectedResult)
    }
  }

  describe("Test fork") {
    it("should pass msgs to both forks") {
      results = Nil

      def createMsg(i: Int, s: String) = s"received by $i: $s"

      val r1 = newStream.map(createMsg(1, _)).foreach(recordResult)
      val r2 = newStream.map(createMsg(2, _)).foreach(recordResult)

      val root = newStream.fork(r1, r2)
      val hello = "Hello, world!"
      root ! hello

      results.reverse should be((1 to 2).map(createMsg(_, hello)))
    }
  }
  describe("Test prepend") {
    val head = newStream.map(s => s"head $s")
    val tail = newStream.map(s => s"tail $s")
    val hello = "Hello, world!"
    val expected = List(s"tail head $hello")

    it("should allow to prepend to a MsgSink") {
      results = Nil

      val joined = head :: tail.foreach(recordResult)
      joined ! hello

      results should be(expected)
    }
    it("should allow prepend to a MsgStream") {
      results = Nil

      val joined = (head :: tail).foreach(recordResult)

      joined ! hello

      results should be(expected)
    }
  }
  describe( "Test join") {
    //def f(i : Int, s : String) = s"$i: $s"
    val i1 = newStream
    val i2 = newStream

    it("should record events from both streams to a MsgSink") {
      results = Nil

      val t = (newStream.foreach( recordResult ))
      val (s1,s2) = (i1,i2) :: t
      s1 ! "a"
      s2 ! "b"

      results.reverse should be (List("a","b"))
    }
    it("should record events from both stream to a stream") {
      val (s1,s2) = (i1,i2) :: newStream
      val o1 = s1.foreach( recordResult )
      val o2 = s2.foreach( recordResult )
    }
  }
}
