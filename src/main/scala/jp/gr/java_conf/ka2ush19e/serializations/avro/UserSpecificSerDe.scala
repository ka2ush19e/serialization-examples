package jp.gr.java_conf.ka2ush19e.serializations.avro

import java.io._

import scala.collection.JavaConversions._

import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import jp.gr.java_conf.ka2ush19e.serializations.avro.schema.User

object UserSpecificSerDe {
  val users =
    List(
      new User("Alice", 128, null),
      new User("Bob", 256, "red"),
      new User("Charles", null, "blue")
    )

  def main(args: Array[String]) {
    println("### json ###")
    serDeWithJson(users)
    println()

    println("### binary ###")
    serDeWithBinary(users)
    println()
  }

  def serDeWithJson(users: Seq[User]) {
    val file = new File("output", "users_specific.json")

    // serialize
    val out = new PrintStream(new FileOutputStream(file))
    val writer = new SpecificDatumWriter[User](classOf[User])
    val encoder = EncoderFactory.get.jsonEncoder(User.SCHEMA$, out)
    users.foreach(writer.write(_, encoder))
    encoder.flush()
    out.println()
    out.flush()
    out.close()

    // deserialize
    val in = new BufferedInputStream(new FileInputStream(file))
    val reader = new SpecificDatumReader[User](classOf[User])
    val decoder = DecoderFactory.get.jsonDecoder(User.SCHEMA$, in)
    try {
      while (true) {
        val user = reader.read(null, decoder)
        println(f"${user.getName}%-10s ${user.getFavoriteNumber}%-5s ${user.getFavoriteColor}%-5s")
      }
    } catch {
      case e: EOFException =>
    }
    in.close()
  }

  def serDeWithBinary(users: Seq[User]) {
    val file = new File("output", "users_specific.avro")

    // serialize
    val writer = new DataFileWriter[User](new SpecificDatumWriter[User](classOf[User]))
    writer.create(User.SCHEMA$, file)
    users.foreach(writer.append)
    writer.close()

    // deserialize
    val reader = new DataFileReader[User](file, new SpecificDatumReader[User](classOf[User]))
    reader.iterator().foreach { datum =>
      println(f"${datum.getName}%-10s ${datum.getFavoriteNumber}%-5s ${datum.getFavoriteColor}%-5s")
    }
    reader.close()
  }
}
