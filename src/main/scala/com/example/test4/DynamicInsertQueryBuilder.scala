import java.sql.{Connection, DriverManager, PreparedStatement}
import com.typesafe.config.ConfigFactory

import java.util.{HashMap => JHashMap}
import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `map AsScala`}


object DynamicInsertQueryBuilder {
  val config = ConfigFactory.load()

  // Read PostgreSQL connection properties
  val url = config.getString("postgres.url")
  val user = config.getString("postgres.user")
  val password = config.getString("postgres.password")
  val driver = config.getString("postgres.driver")

  // Register the PostgreSQL driver
  Class.forName(driver)

  // Create a connection
  val connection: Connection = DriverManager.getConnection(url, user, password)

  def insertSingleRows(tableName: String, rows: JHashMap[String, Any]) {

    val columns = rows.keys.mkString(", ")
    val values = rows.values.map(prepareValue).mkString(", ")
    val insertQuery = s"INSERT INTO $tableName ($columns) VALUES ($values);"
    println(insertQuery)

    try {
      // Create a prepared statement with the dynamically constructed SQL query
      val preparedStatement: PreparedStatement = connection.prepareStatement(insertQuery)

      // Execute the SQL query
      preparedStatement.executeUpdate()

      println("Record inserted successfully.")
    } finally {
      // Close the database connection
      //connection.close()
    }

  }

  def prepareValue(value: Any): String = value match {
    case str: String => s"'$str'"
    case bool: Boolean => bool.toString
    case _ => throw new IllegalArgumentException(s"Unsupported data type: ${value.getClass}")
  }

}
