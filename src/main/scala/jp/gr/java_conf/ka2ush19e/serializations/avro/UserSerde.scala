package jp.gr.java_conf.ka2ush19e.serializations.avro

import java.io.File

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import jp.gr.java_conf.ka2ush19e.serializations.avro.schema.User

object UserSerDe {
  def main(args: Array[String]) {
    println("### SerDe with code generation ###")
    serDeWithCodeGeneration()
    println()

    println("### SerDe without code generation ###")
    serDeWithoutCodeGeneration()
    println()
  }

  def serDeWithCodeGeneration() {
    val user1 = new User()
    user1.setName("Alice")
    user1.setFavoriteNumber(128)

    val user2 = new User("Bob", 256, "red")

    val user3 =
      User.newBuilder()
        .setName("Charles")
        .setFavoriteNumber(null)
        .setFavoriteColor("blue")
        .build()

    val file = new File("output", "users1.avro")

    // serialize
    val userDatumWriter = new SpecificDatumWriter[User](classOf[User])
    val dataFileWriter = new DataFileWriter[User](userDatumWriter)
    dataFileWriter.create(user1.getSchema, file)
    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.append(user3)
    dataFileWriter.close()

    // deserialize
    val userDatumReader = new SpecificDatumReader[User](classOf[User])
    val dataFileReader = new DataFileReader[User](file, userDatumReader)
    dataFileReader.iterator().foreach(println)
  }

  def serDeWithoutCodeGeneration() {
    val schema = new Schema.Parser().parse(new File("src/main/avro", "user.avsc"))

    val user1 = new GenericData.Record(schema)
    user1.put("name", "Alice")
    user1.put("favorite_number", 128)

    val user2 = new GenericData.Record(schema)
    user2.put("name", "ボブ")
    user2.put("favorite_number", 256)

    val file = new File("output", "users2.avro")

    // serialize
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema, file)
    dataFileWriter.append(user1)
    dataFileWriter.append(user2)
    dataFileWriter.close()

    // deserialize
    val userDatumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](file, userDatumReader)
    dataFileReader.iterator().foreach(println)
  }
}
