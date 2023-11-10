import io.circe.{ACursor, HCursor, Json}
import io.circe.parser._

import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.collection.mutable.ListBuffer
import scala.io.Source

object test3 {

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

  def main(args: Array[String]): Unit = {
    val schemaPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test3/inputSchema.json"
    val schemaString = Source.fromFile(schemaPath).mkString
    val jsonSchema: Json = parse(schemaString) match {
      case Right(json) => json
      case Left(error) =>
        println(s"Error parsing INPUT JSON schema: $error")
        Json.obj() // Return an empty JSON object in case of an error
    }

    val eventPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test3/event.json"
    val eventString = Source.fromFile(eventPath).mkString
    val incomingEvent = parse(eventString).getOrElse(Json.obj())

    val data_mappingPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test3/data_mapping.json"
    val mappingString = Source.fromFile(data_mappingPath).mkString
    val dataMapping = parse(mappingString).getOrElse(Json.obj())
    //println("dataMapping = " + dataMapping)

    val isValid = validate(jsonSchema, incomingEvent)
    if (isValid) {
      println("Valid JSON event")
    } else {
      println("Invalid JSON event")
    }

    //    println(incomingEvent)
    //
//        val Survey_ID = incomingEvent.hcursor.downField("surveyId").focus
//        println("Survey_ID = " + Survey_ID)

    val result = deserializeEvent(incomingEvent.hcursor, dataMapping)
    // Print the result (list of maps)
    //result.foreach(println)
    println(result)

    // println(result)


  }

  def deserializeEvent(eventCursor: ACursor, dataMapping: Json): ListBuffer[Map[String, String]] = {
    val dataMappingCursor = dataMapping.hcursor
    println("--------------------------------")
    //println(dataMappingCursor.value)
    val keys = dataMappingCursor.keys.getOrElse(Nil).toList
    //println(keys)

    val result = ListBuffer.empty[Map[String, String]]

    keys.foreach { key =>
      val fieldPath = dataMappingCursor.downField(key).as[String].getOrElse("")
      //println(fieldPath)
      val eventField = getNestedField(eventCursor, fieldPath)

      result += Map(key -> eventField)
    }

    result
  }

  def getNestedField(cursor: ACursor, fieldPath: String): String = {
    val pathParts = fieldPath.split('.')
    val finalCursor = pathParts.foldLeft(cursor) { (currentCursor, part) =>
      currentCursor.downField(part)
    }
    finalCursor.as[String].getOrElse("")
  }


}
