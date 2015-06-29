package jp.gr.java_conf.ka2ush19e.serializations.thrift

import com.twitter.finagle.Thrift
import com.twitter.util.{Await, Future}

import jp.gr.java_conf.ka2ush19e.serializations.thrift.company._

class CompanyServer extends CompanyService.FutureIface {
  val company = Company("FooTechnology")
  var employees = List(Employee("Alice", Job.Engineer), Employee("Bob", Job.Analyst))

  override def get(): Future[Company] = {
    println("get()")
    Future.value(company)
  }

  override def getEmployees(): Future[Seq[Employee]] = {
    println("getEmployees")
    Future.value(employees)
  }
  override def addEmployee(name: String, job: Job): Future[Unit] = {
    println("addEmployee")
    Future.value()
  }

  override def findEmployeeByName(name: String): Future[Employee] = {
    println("findEmployeeByName")
    employees.find(_.name == name) match {
      case Some(e) => Future.value(e)
      case _ => throw new EmployeeNotFound(name)
    }
  }
  override def findEmployeeByJob(job: Job): Future[Employee] = {
    println("findEmployeeByJob")
    employees.find(_.job == job) match {
      case Some(e) => Future.value(e)
      case _ => throw new EmployeeNotFound("")
    }
  }
}

object CompanyServer {
  def main(args: Array[String]) {
    val server = Thrift.serveIface("localhost:9090", new CompanyServer())
    Await.ready(server)
  }
}
