package jp.gr.java_conf.ka2ush19e.serializers.thrift

import com.twitter.finagle.Thrift
import com.twitter.util.Await

import jp.gr.java_conf.ka2ush19e.serializers.thrift.company.{CompanyService, EmployeeNotFound}

object CompanyClient {
  def main(args: Array[String]) {
    val client = Thrift.newIface[CompanyService.FutureIface]("localhost:9090")

    val company = Await.result(client.get())
    println(company.name)

    val employees = Await.result(client.getEmployees())
    println(employees)

    val alice = Await.result(client.findEmployeeByName("Alice"))
    println(alice)

    try {
      Await.result(client.findEmployeeByName("Charles"))
    } catch {
      case e: EmployeeNotFound => println(s"${e.name} is not a employee")
    }
  }
}
