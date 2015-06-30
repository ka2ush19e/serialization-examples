package jp.gr.java_conf.ka2ush19e.serializers.protobuf

import java.io.{File, FileInputStream, FileOutputStream}

import scala.collection.JavaConversions._

import jp.gr.java_conf.ka2ush19e.serializers.protobuf.CompanyProto.{Company, Employee, Job}

object CompanySerDe {
  def main(args: Array[String]) {

    val company1 = Company.newBuilder()
      .setName("FooTechnology")
      .addEmployee(Employee.newBuilder().setName("Alice").setJob(Job.ENGINEER).build())
      .addEmployee(Employee.newBuilder().setName("Bob").setJob(Job.SALES).build())
      .build()

    val company2 = Company.newBuilder()
      .setName("BarScience")
      .addEmployee(Employee.newBuilder().setName("Chris").setJob(Job.ANALYST).build())
      .build()

    // serialize
    val file = new File("output", "company.proto")
    val fos = new FileOutputStream(file)
    company1.writeDelimitedTo(fos)
    company2.writeDelimitedTo(fos)
    fos.close()

    // deserialize
    val fis = new FileInputStream(file)
    Iterator.continually(Company.parseDelimitedFrom(fis)).takeWhile(_ != null).foreach { c =>
      println(s"${c.getName}'s employees")
      c.getEmployeeList.foreach { e =>
        println(f"  ${e.getName}%-8s ${e.getJob}")
      }
    }
    fis.close()
  }
}
