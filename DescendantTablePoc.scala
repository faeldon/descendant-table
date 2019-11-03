// Databricks notebook source
// MAGIC %md ## Access Control Lookup Table Using Spark GraphX/Pregel API

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md ##### Example Data

// COMMAND ----------

// Keep case classes in separate cells (https://kb.databricks.com/notebooks/common-errors-in-notebooks.html)
type Role = String
case class Employee(name: String, role: Role)

// COMMAND ----------

val employeeRawData = Array(
  (1L, "Steve", "Jobs", "CEO", None),
  (2L, "Leslie", "Lamport", "CTO", Some(1L)),
  (3L, "Jason", "Fried", "Manager", Some(1L)),
  (4L, "Joel", "Spolsky", "Manager", Some(2L)),
  (5L, "Jeff", "Dean", "Lead", Some(4L)),
  (6L, "Martin", "Odersky", "Sr.Dev", Some(5L)),
  (7L, "Linus", "Trovalds", "Dev", Some(6L)),
  (8L, "Steve", "Wozniak", "Dev", Some(6L)),
  (9L, "Matei", "Zaharia", "Dev", Some(6L)),
  (10L, "James", "Faeldon", "Intern", Some(7L))
)

val employeeDf = sc.parallelize(employeeRawData, 4).toDF(
  "employeeId",
  "firstName",
  "lastName",
  "role",
  "supervisorId"
).cache()

val verticesRdd: RDD[(VertexId, Employee)] = employeeDf
  .select($"employeeId", concat($"firstName", lit(" "), $"lastName"), $"role")
  .rdd.map(emp => (emp.getLong(0), Employee(emp.getString(1), emp.getString(2))))

val edgesRdd: RDD[Edge[String]] = employeeDf
  .filter($"supervisorId".isNotNull) // Remove vertices without supervisor, in Scala None === Null
  .select($"supervisorId", $"employeeId", $"role") // First column is supervisorID (not employeeId), since direction of edge is top-down
  .rdd.map(emp => Edge(emp.getLong(0), emp.getLong(1), emp.getString(2))) // Edge property is the Role


// COMMAND ----------

// Define a default employee in case there are missing employee referenced in Graph
val missingEmployee = Employee("John Doe", "Unknown")

// Let's build the graph model
val employeeGraph: Graph[Employee, String] = Graph(verticesRdd, edgesRdd, missingEmployee)

// COMMAND ----------

// MAGIC %md ##### Define Superstep

// COMMAND ----------

// The structure of the message to be passed to vertices
case class EmployeeMessage(
  currentId: Long, // Tracks the most recent vertex appended to path and used for flagging isCyclic
  level: Int, // The number of up-line supervisors (level in reporting heirarchy)
  head: String, // The top-most supervisor
  path: List[String], // The reporting path to the the top-most supervisor
  isCyclic: Boolean, // Is the reporting structure of the employee cyclic
  isLeaf: Boolean // Is the employee rank and file (no down-line reporting employee)
)

// The structure of the vertex values of the graph
case class EmployeeValue(
  name: String, // The employee name
  currentId: Long, // Initial value is the employeeId
  level: Int, // Initial value is zero
  head: String, // Initial value is this employee's name
  path: List[String], // Initial value contains this employee's name only
  isCyclic: Boolean, // Initial value is false
  isLeaf: Boolean // Initial value is true
)


// COMMAND ----------

// Initialize the employee vertices
val employeeValueGraph: Graph[EmployeeValue, String] = employeeGraph.mapVertices { (id, v) =>
  EmployeeValue(
    name = v.name,
    currentId = id,
    level = 0,
    head = v.name,
    path = List(v.name),
    isCyclic = false,
    isLeaf = false
  )
}

// COMMAND ----------

/**
  * Step 1: Mutate the value of the vertices, based on the message received
  */
def vprog(
  vertexId: VertexId, 
  value: EmployeeValue, 
  message: EmployeeMessage
): EmployeeValue = {
  
  if (message.level == 0) { //superstep 0 - initialize
    value.copy(level = value.level + 1)
  } else if (message.isCyclic) { // set isCyclic
    value.copy(isCyclic = true)
  } else if (!message.isLeaf) { // set isleaf    
    value.copy(isLeaf = false)  
  } else { // set new values
    value.copy(
      currentId = message.currentId,
      level = value.level + 1,
      head = message.head,
      path = value.name :: message.path
    )
  }
}


// COMMAND ----------

/**
  * Step 2: For all triplets that received a message -- meaning, any of the two vertices 
  * received a message from the previous step -- then compose and send a message.
  */
def sendMsg(
  triplet: EdgeTriplet[EmployeeValue, String]
): Iterator[(VertexId, EmployeeMessage)] = {
  
  val src = triplet.srcAttr
  val dst = triplet.dstAttr
  
  // Handle cyclic reporting structure
  if (src.currentId == triplet.dstId || src.currentId == dst.currentId) {
    if (!src.isCyclic) { // Set isCyclic
      Iterator((triplet.dstId, EmployeeMessage(
        currentId = src.currentId, 
        level = src.level, 
        head = src.head, 
        path = src.path, 
        isCyclic = true,
        isLeaf = src.isLeaf
      )))
    } else { // Already marked as isCyclic (possibly, from previous superstep) so ignore
      Iterator.empty
    }
  } else { // Regular reporting structure
    if (src.isLeaf) { // Initially every vertex is leaf. Since this is a source then it should NOT be a leaf, update
      Iterator((triplet.srcId, EmployeeMessage(
        currentId = src.currentId,
        level = src.level,
        head = src.head, 
        path = src.path, 
        isCyclic = false, 
        isLeaf = false // This is the only important value here
      )))
    } else { // Set new values by propagating source values to destination
      //Iterator.empty
      Iterator((triplet.dstId, EmployeeMessage(
        currentId = src.currentId,
        level = src.level,
        head = src.head, 
        path = src.path, 
        isCyclic = false, // Set to false so that cyclic updating is ignored in vprog
        isLeaf = true // Set to true so that leaf updating is ignored in vprog
      )))
    }
  }
}

// COMMAND ----------

/**
  * Step 3: Merge all inbound messages to a vertex. No special merging needed for this use case.
  */
def mergeMsg(msg1: EmployeeMessage, msg2: EmployeeMessage): EmployeeMessage = msg2

// COMMAND ----------

// MAGIC %md ##### Run Algorithm

// COMMAND ----------

val initialMsg = EmployeeMessage(
    currentId = 0L, 
    level = 0, 
    head = "", 
    path = Nil, 
    isCyclic = false, 
    isLeaf = true
)

val results = employeeValueGraph.pregel(
  initialMsg,
  Int.MaxValue,
  EdgeDirection.Out
)(
  vprog,
  sendMsg,
  mergeMsg
)

val resultDf = results
  .vertices.map { case (id, v) => (id, v.name, v.level, v.head, v.path.reverse.mkString(">"), v.isCyclic, v.isLeaf) }
  .toDF("id", "employee", "level", "head", "path", "cyclic", "leaf")

display(resultDf)

// COMMAND ----------

val acl = results.
  vertices.flatMap { case (id, v) => 
    v.path.map(p => ((id, v.name, p, v.level, v.head, v.path.reverse.mkString(">"), v.isCyclic, v.isLeaf)))
  }.toDF("id", "employee", "access", "level", "head", "path", "cyclic", "leaf")
display(acl)
