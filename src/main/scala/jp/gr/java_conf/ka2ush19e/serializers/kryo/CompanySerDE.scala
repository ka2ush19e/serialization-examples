package jp.gr.java_conf.ka2ush19e.serializers.kryo

import java.io.{File, FileInputStream, FileOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.ScalaKryoInstantiator

object CompanySerDe {

  class Company(val name: String, val employees: List[Employee])
  class Employee(val name: String, val job: Job.Value)
  object Job extends Enumeration {
    val Sales, Engineer, Analyst = Value
  }

  def main(args: Array[String]) {

    val company = new Company(
      "FooTechnology",
      List(new Employee("Alice", Job.Engineer), new Employee("Bob", Job.Analyst))
    )

    val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(false)
    val kryo = instantiator.newKryo()

    // serialize
    val file = new File("output", "company.kryo")
    val output = new Output(new FileOutputStream(file))
    kryo.writeObject(output, company)
    output.close()

    // deserialize
    val input = new Input(new FileInputStream(file))
    val deserialized = kryo.readObject(input, classOf[Company])
    input.close()

    println(s"${deserialized.name}'s employees")
    deserialized.employees.foreach { e =>
      println(f"  ${e.name}%-8s ${e.job}")
    }
  }
}
