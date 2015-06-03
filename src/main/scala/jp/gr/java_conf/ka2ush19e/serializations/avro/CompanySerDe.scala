package jp.gr.java_conf.ka2ush19e.serializations.avro

import java.io.File

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.file.{ DataFileReader, DataFileWriter }
import org.apache.avro.generic._
import org.apache.avro.specific.{ SpecificDatumReader, SpecificDatumWriter }

import jp.gr.java_conf.ka2ush19e.serializations.avro.schema.{ Company, Employee, Job }

object CompanySerDe {
  def main(args: Array[String]) {
    println("### SerDe with code generation ###")
    serDeWithCodeGeneration()
    println()

    println("### SerDe without code generation ###")
    serDeWithoutCodeGeneration()
    println()
  }

  def serDeWithCodeGeneration() {
    val company1 = new Company("FooTechnology", List(
      new Employee("Alice", Job.Engineer),
      new Employee("Bod", Job.Sales)
    ))
    val company2 = new Company("BarScience", List(
      new Employee("Chris", Job.Analyst)
    ))

    val file = new File("output", "company1.avro")

    // serialize
    val userDatumWriter = new SpecificDatumWriter[Company](classOf[Company])
    val dataFileWriter = new DataFileWriter[Company](userDatumWriter)
    dataFileWriter.create(company1.getSchema, file)
    dataFileWriter.append(company1)
    dataFileWriter.append(company2)
    dataFileWriter.close()

    // deserialize
    val userDatumReader = new SpecificDatumReader[Company](classOf[Company])
    val dataFileReader = new DataFileReader[Company](file, userDatumReader)
    dataFileReader.iterator().foreach { c =>
      println(s"${c.getName}'s employees")
      c.getEmployees.foreach { e =>
        println(f"  ${e.getName}%-8s ${e.getJob}")
      }
    }
  }

  def serDeWithoutCodeGeneration() {
    val companySchema = new Schema.Parser().parse(new File("src/main/avro", "company.avsc"))

    val employeesSchema = companySchema.getField("employees").schema()
    val employeeSchema = employeesSchema.getElementType

    val employees = new GenericData.Array[GenericRecord](2, employeesSchema)
    val employee1 = new GenericData.Record(employeeSchema)
    employee1.put("name", "Alice")
    employee1.put("job", "Engineer")
    employees.add(employee1)

    val employee2 = new GenericData.Record(employeeSchema)
    employee2.put("name", "Bob")
    employee2.put("job", "Sales")
    employees.add(employee2)

    val company1 = new GenericData.Record(companySchema)
    company1.put("name", "FooTechnology")
    company1.put("employees", employees)

    val file = new File("output", "company2.avro")

    // serialize
    val datumWriter = new GenericDatumWriter[GenericRecord](companySchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(companySchema, file)
    dataFileWriter.append(company1)
    dataFileWriter.close()

    // deserialize
    val userDatumReader = new GenericDatumReader[GenericRecord](companySchema)
    val dataFileReader = new DataFileReader[GenericRecord](file, userDatumReader)
    dataFileReader.iterator().foreach { r =>
      println(s"${r.get("name")}'s employees")
      r.get("employees") match {
        case es: GenericData.Array[GenericData.Record ] =>
          es.iterator().foreach { e => println(f"""  ${e.get("name")}%-8s ${e.get("job")}""") }
        case _ =>
      }
    }
  }
}
