import io.circe.Decoder.decodeString
import io.circe.parser._
import io.circe.{ACursor, Json, JsonObject}

import java.util.{HashMap => JHashMap}
import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.collection.mutable.ListBuffer
import scala.io.Source

// Define a case class to represent the field metadata

object test6 {


  def validate(jsonSchema: Json, jsonData: Json): Boolean = {
    val schema = jsonSchema.hcursor
    val data = jsonData.hcursor

    // Check if all keys in the schema are present in the data JSON
    val schemaKeys = schema.keys.getOrElse(Set.empty)
    val dataKeys = data.keys.getOrElse(Set.empty)
    val allKeysPresent = schemaKeys.forall(dataKeys.contains)

    // Validate the schema recursively for nested objects
    val validSchema = schemaKeys.forall { key =>
      val schemaValue = schema.downField(key).focus
      val dataValue = data.downField(key).focus


      (schemaValue, dataValue) match {
        case (Some(schemaObj), Some(dataObj)) if schemaObj.isObject =>
          validate(schemaObj, dataObj)
        case (Some(schemaVal), Some(dataVal)) =>
          schemaVal.isString && dataVal.isString
        case _ => false
      }
    }
    allKeysPresent && validSchema
  }

  def getString(key: String, json: Json): String = {
    val splitKey = key.split('.')
    val result = splitKey.foldLeft(json) { (currentJson, part) =>
      currentJson.hcursor.downField(part).focus.getOrElse(Json.Null)
    }
    result.asString.getOrElse("")
  }

  def getBoolean(key: String, data: ACursor): Boolean = {
    val splitKey = key.split('.')
    val finalCursor = splitKey.foldLeft(data) { (currentCursor, part) =>
      currentCursor.downField(part)
    }
    finalCursor.as[Boolean].getOrElse(false)
  }


  def main(args: Array[String]): Unit = {
    val schemaPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test4/inputSchema.json"
    val schemaString = Source.fromFile(schemaPath).mkString
    val jsonSchema: Json = parse(schemaString) match {
      case Right(json) => json
      case Left(error) =>
        println(s"Error parsing INPUT JSON schema: $error")
        Json.obj() // Return an empty JSON object in case of an error
    }

    val eventPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test4/event.json"
    val incomingEventString = Source.fromFile(eventPath).mkString
    val jsonData = parse(incomingEventString).getOrElse(Json.obj())
    val data = parse(incomingEventString).getOrElse(Json.obj()).hcursor

    val isValid = validate(jsonSchema, jsonData)
    if (isValid) {
      println("Valid JSON event")
    } else {
      println("Invalid JSON event")
    }

    val data_mappingPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test4/data_mapping1.json"
    val mappingString = Source.fromFile(data_mappingPath).mkString
    val dataMapping = parse(mappingString).getOrElse(Json.obj())


    val dataMappingKeys = dataMapping.hcursor.keys.getOrElse(Set.empty)
    println(dataMappingKeys)

    val singleRowValues = new JHashMap[String, Any]()
    val multipleRows: ListBuffer[JHashMap[String, Any]] = new ListBuffer[JHashMap[String, Any]]()




    dataMappingKeys.foreach { key =>
      if (dataMapping.hcursor.downField(key).get[String]("rowtype").getOrElse("") == "single") {
        /**
         * call a method to process direct key values
         */
        val rowstructure = dataMapping.hcursor.downField(key).downField("rowstructure").focus.getOrElse(Json.arr())

        // Check if rowstructure is an array
        if (rowstructure.isArray) {
          // Iterate through the elements in the rowstructure array
          rowstructure.asArray.foreach { rowObjArray =>
            rowObjArray.foreach { rowObj =>
              val columnName = rowObj.hcursor.get[String]("columnName").getOrElse(null)
              val datatype = rowObj.hcursor.get[String]("datatype").getOrElse(null)
              val source = rowObj.hcursor.get[String]("source").getOrElse(null)
              val nullable = rowObj.hcursor.get[Boolean]("nullable").getOrElse(false)

              if (datatype == "String") {
                val result = getString(source, jsonData)
                if (!nullable && (result == null || result.isEmpty)) {
                  throw new Exception("Field cannot be null or empty for the " + source)
                }
                singleRowValues.put(columnName, result)
              }

              if (datatype == "Boolean") {
                val result = getBoolean(source, data)
                singleRowValues.put(columnName, result)
              }

              /**
               * todo : create remaining methods to call in the different dataType of JSON
               */
            }

          }
        }

      }


      if (dataMapping.hcursor.downField(key).get[String]("rowtype").getOrElse("") == "multiple") {

        val dataSourceKey = dataMapping.hcursor.downField(key).get[String]("source").getOrElse(null)
        val dataSource = jsonData.hcursor.downField(dataSourceKey).focus
        dataSource match {
          case Some(answersJson) =>
            //convert to jsonObject
            val jsonObject = answersJson.asObject.getOrElse(JsonObject.empty)
            //convert to jsonArray
            val jsonArray = jsonObject.values.toArray

            jsonArray.foreach { jsonValue =>

              if (jsonValue.isObject) {

                val rowstructure = dataMapping.hcursor.downField(key).downField("rowstructure").focus.getOrElse(Json.arr())
                if (rowstructure.isArray) {
                  // Iterate through the elements in the rowstructure array
                  rowstructure.asArray.foreach { rowObjArray =>
                    val RowValue = new JHashMap[String, Any]()
                    rowObjArray.foreach { rowObj =>
                      val columnName = rowObj.hcursor.get[String]("columnName").getOrElse(null)
                      val datatype = rowObj.hcursor.get[String]("datatype").getOrElse(null)
                      val source = rowObj.hcursor.get[String]("source").getOrElse(null)
                      val nullable = rowObj.hcursor.get[Boolean]("nullable").getOrElse(false)

                      if (datatype == "String") {
                        val result = getString(source, jsonValue)
                        if (!nullable && (result == null || result.isEmpty)) {
                          throw new Exception("Field cannot be null or empty for the " + source)
                        }
                        //println(s"$columnName : $result")
                        RowValue.put(columnName, result)
                      }

                    }
                    multipleRows += RowValue

                  }
                }
              }
            }
            val numberOfAnswers = jsonObject.size

            println(s"Number of answers object: $numberOfAnswers")

          case None =>
            println("No 'answers' field found in the JSON.")
        }
      }
    }

    /**
     * Final survey table data
     */
    println(singleRowValues.toString)

    /**
     * Final question table data
     */
    println("Number of rows in MultipleValues "+ multipleRows.size)
    println(multipleRows)

  }
}






