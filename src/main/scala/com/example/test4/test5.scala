import io.circe.Decoder.decodeString
import io.circe.KeyDecoder.decodeKeyString
import io.circe.{ACursor, Decoder, HCursor, Json, JsonObject, Printer}
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.syntax.EncoderOps
import shapeless.Lazy.apply

import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.util.{HashMap => JHashMap}
import scala.collection.JavaConverters.mapAsScalaMapConverter

// Define a case class to represent the field metadata

object test5 {

  //case class RowStructure(columnName: String, datatype: String, source: String, nullable: Boolean)


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

  // Simulated getString method
  //  def getString(key: String, data: HCursor): String = {
  //    //val result = data.downField(key).focus
  //    val res = data.downField(key).focus.flatMap(_.asString).getOrElse(null)
  //    res
  //  }

  //  def getString(key: String, data: ACursor): String = {
  //    val splitKey = key.split('.')
  //    val finalCursor = splitKey.foldLeft(data) { (currentCursor, part) =>
  //      currentCursor.downField(part)
  //    }
  //    finalCursor.as[String].getOrElse("")
  //  }

  def getString(key: String, json: Json): String = {
    val splitKey = key.split('.')

    // Use a foldLeft to navigate through the JSON
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

    //    println("++++++++++++++++++++++++++++++++\n" + data)
    //    println("++++++++++++++++++++++++++++++++\n")

    val isValid = validate(jsonSchema, jsonData)
    if (isValid) {
      println("Valid JSON event")
    } else {
      println("Invalid JSON event")
    }

    val data_mappingPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test4/data_mapping1.json"
    val mappingString = Source.fromFile(data_mappingPath).mkString
    val dataMapping = parse(mappingString).getOrElse(Json.obj())
    //println("dataMapping = " + dataMapping)


    val dataKeys = jsonData.hcursor.keys.getOrElse(Set.empty)
    println(dataKeys)
    dataKeys.foreach { key =>
      //println(key)
      if (key == "answers") {
        println("inside answers")


      }
    }


    /**
    val parsedJson = parse(incomingEventString)

    parsedJson match {
      case Right(json) =>
        // Use the provided JSON traversal methods to extract values dynamically
        val answers = json.hcursor.downField("answers").focus

        // Check if the "answers" field exists
        answers match {
          case Some(answersJson) =>
            // Process the "answers" field or its contents as needed
            println(answersJson.isObject)
            val jsonObject = answersJson.asObject.getOrElse(JsonObject.empty)
            //println(jsonObject)
            val jsonArray = jsonObject.values.toArray

            //jsonArray.foreach(f => println(f))

            jsonArray.foreach { jsonValue =>
              // Check if the JSON value is an object
              if (jsonValue.isObject) {
                // Extract the "qid" key from the JSON value
                /**
                 * dynamic extraction
                 * val questionId = getString("qid", jsonValue)
                 * val question = getString("payload.question", jsonValue)
                 * println(questionId)
                 * println(question)
                 */

                val questionId = getString("qid", jsonValue)
                println ("questionId : "+  questionId)



                val question = getString("payload", jsonValue)
                println ("question : "+ question)



                //println(jsonValue)


//                val qidOption = jsonValue.hcursor.get[String]("qid")
//                println(qidOption)
//                val questioResponceType = jsonValue.hcursor.get[String]("payload.responseType")
//                println(questioResponceType)


              }
            }


            // Get the number of key-value pairs
            val numberOfAnswers = jsonObject.size

            println(s"Number of answers: $numberOfAnswers")
            //println(jsonObject.keys)

          //            jsonObject.keys.foreach { key =>
          //              println("=============================")
          //              println(key)
          //              println(key.getClass)
          //
          //            }


          //println("Answers:")
          //println(answersJson.spaces2) // Print the "answers" field and its contents
          case None =>
            println("No 'answers' field found in the JSON.")
        }

      case Left(error) =>
        println(s"Error parsing JSON: $error")
    }
    **/


    val dataMappingKeys = dataMapping.hcursor.keys.getOrElse(Set.empty)
    println(dataMappingKeys)

    val singleRowValues = new JHashMap[String, Any]()
    val multipleRows: ListBuffer[JHashMap[String, Any]] = new ListBuffer[JHashMap[String, Any]]()




    dataMappingKeys.foreach { key =>
      if (dataMapping.hcursor.downField(key).get[String]("rowtype").getOrElse("") == "single") {
        /**
         * call a method to process direct key values
         */
        val source = dataMapping.hcursor.downField(key).get[String]("source").getOrElse(null)
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
                //println(s"$columnName : $result")
                singleRowValues.put(columnName, result)
              }

              if (datatype == "Boolean") {
                val result = getBoolean(source, data)
                //println(s"$columnName: $result")
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
        /**
         * todo : call a method to process object key-values
         */
        // println(key)


        val dataSourceKey = dataMapping.hcursor.downField(key).get[String]("source").getOrElse(null)
        println(dataSourceKey)

        val dataSource = jsonData.hcursor.downField(dataSourceKey).focus


        dataSource match {
          case Some(answersJson) =>
            //convert to jsonObject
            val jsonObject = answersJson.asObject.getOrElse(JsonObject.empty)
            //convert to jsonArray
            val jsonArray = jsonObject.values.toArray

            jsonArray.foreach { jsonValue =>

              if (jsonValue.isObject) {

                println("======================")
                println(jsonValue)
                println("======================")

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

            println(s"Number of answers: $numberOfAnswers")

          case None =>
            println("No 'answers' field found in the JSON.")


        }























        val rowstructure = dataMapping.hcursor.downField(key).downField("rowstructure").focus.getOrElse(Json.arr())
        //println(rowstructure.isString)
      }


    }

    /**
     * Final survey yable data
     */
    //println(singleRowValues.toString)
    //println(RowValue)
    println("Number of rows in MultipleValues "+ multipleRows.size)
    println(multipleRows)

  }






  //    val rowtype = dataMapping.hcursor.downField("survey_table").get[String]("rowtype").getOrElse("default_rowtype")
  //    val source = dataMapping.hcursor.downField("survey_table").get[String]("source").getOrElse("default_source")
  //    val rowstructure = dataMapping.hcursor.downField("survey_table").get[Json]("rowstructure").getOrElse(Json.obj())
  //
  //    // Now you have rowtype, source, and rowstructure in separate variables
  //    println("Rowtype: " + rowtype)
  //    println("Source: " + source)
  //    println("Rowstructure:")
  //    println(rowstructure.spaces2)


  //println("dataMapping = " + dataMapping.getClass.getName)
  //println("dataMapping = " + dataMapping.printWith(Printer.spaces2) + " (Type: " + dataMapping.getClass.getName + ")")


  //    val mappingKeys = dataMapping.hcursor.keys.getOrElse(Set.empty).toString()
  //    println(mappingKeys)
  //
  //    val surveyTable = dataMapping.hcursor.downField(mappingKeys).focus.getOrElse(Json.obj())
  //
  //    val questionTable = dataMapping.hcursor.downField("question_table").focus.getOrElse(Json.obj())

  // Print the two separate JSON objects
  //    println("Survey Table JSON:")
  // println(surveyTable.spaces2)
  //
  //    println("Question Table JSON:")
  //    println(questionTable.spaces2)


}






