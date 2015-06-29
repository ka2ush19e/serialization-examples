namespace java jp.gr.java_conf.ka2ush19e.serializations.thrift.company

enum Job {
  SALES = 1
  ENGINEER = 2
  ANALYST = 3
}

struct Company {
  1: string name
}

struct Employee {
  1: string name
  2: Job job
}

exception EmployeeNotFound {
  1: string name
}

service CompanyService {
  Company get()
  list<Employee> getEmployees()
  Employee findEmployeeByName(1: string name) throws(1: EmployeeNotFound ex)
  Employee findEmployeeByJob(1: Job job) throws(1: EmployeeNotFound ex)
  void addEmployee(1: string name, 2: Job job)
}

