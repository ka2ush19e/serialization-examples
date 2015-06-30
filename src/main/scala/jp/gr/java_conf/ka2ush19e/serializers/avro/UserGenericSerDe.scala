package jp.gr.java_conf.ka2ush19e.serializers.avro

import java.io._

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

object UserGenericSerDe {
  val schema = new Schema.Parser().parse(new File("src/main/avro", "user.avsc"))
  val users =
    List(
      createUser("Alice", Some(128), None),
      createUser("Bob", Some(256), Some("red")),
      createUser("Charles", None, Some("blue"))
    )

  def createUser(name: String, favoriteNumberOpt: Option[Int], favoriteColorOpt: Option[String]): GenericData.Record = {
    val user = new GenericData.Record(schema)
    user.put("name", name)
    favoriteNumberOpt match {
      case Some(n) => user.put("favorite_number", n)
      case _ =>
    }
    favoriteColorOpt match {
      case Some(c) => user.put("favorite_color", c)
      case _ =>
    }
    user
  }

  def main(args: Array[String]) {
    println("### json ###")
    serDeWithJson()
    println()

    println("### binary ###")
    serDeWithBinary()
    println()
  }

  def serDeWithJson() {
    val file = new File("output", "users_generic.json")

    // serialize
    val out = new PrintStream(new FileOutputStream(file))
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get.jsonEncoder(schema, out)

    users.foreach(writer.write(_, encoder))
    encoder.flush()
    out.println()
    out.flush()
    out.close()

    // deserialize
    val in = new BufferedInputStream(new FileInputStream(file))
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get.jsonDecoder(schema, in)
    try {
      while (true) {
        val datum = reader.read(null, decoder)
        println( f"""${datum.get("name")}%-10s ${datum.get("favorite_number")}%-5s ${datum.get("favorite_color")}%-5s""")
      }
    } catch {
      case e: EOFException =>
    }
    in.close()
  }

  def serDeWithBinary() {
    val file = new File("output", "users_generic.avro")

    // serialize
    val writer = new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord](schema))
    writer.create(schema, file)
    users.foreach(writer.append)
    writer.close()

    // deserialize
    val reader = new DataFileReader[GenericRecord](file, new GenericDatumReader[GenericRecord](schema))
    reader.iterator().foreach { datum =>
      println( f"""${datum.get("name")}%-10s ${datum.get("favorite_number")}%-5s ${datum.get("favorite_color")}%-5s""")
    }
    reader.close()
  }
}
