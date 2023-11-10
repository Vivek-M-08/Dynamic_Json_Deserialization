import io.circe.{ACursor, Decoder, HCursor, Json}
import io.circe.parser._

import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.util.{HashMap => JHashMap}
import scala.collection.JavaConverters.mapAsScalaMapConverter

// Define a case class to represent the field metadata

object test4 {

  case class dataMappingMetadata(`type`: String, key: String, nullable: Boolean)

  implicit val fieldMetadataDecoder: Decoder[dataMappingMetadata] = Decoder.forProduct3("type", "key", "nullable")(dataMappingMetadata.apply)


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
  def getString(key: String, data: HCursor): String = {
    //val result = data.downField(key).focus
    val res = data.downField(key).focus.flatMap(_.asString).getOrElse(null)
    res
  }

  def getObject(key: String, data: ACursor): String = {

    val splitKey = key.split('.')
    val finalCursor = splitKey.foldLeft(data) { (currentCursor, part) =>
      currentCursor.downField(part)
    }
    finalCursor.as[String].getOrElse("")
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
    val eventString = Source.fromFile(eventPath).mkString
    val incomingEvent = parse(eventString).getOrElse(Json.obj())
    val jsonData = incomingEvent.hcursor

    val data_mappingPath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/test4/data_mapping.json"
    val mappingString = Source.fromFile(data_mappingPath).mkString
    val dataMapping = parse(mappingString).getOrElse(Json.obj())
    //println("dataMapping = " + dataMapping)

    val isValid = validate(jsonSchema, incomingEvent)
    if (isValid) {
      println("Valid JSON event")
    } else {
      println("Invalid JSON event")
    }

    // Create a Map to store field metadata
    val fieldMetadataMap: Map[String, dataMappingMetadata] = dataMapping.hcursor.keys.getOrElse(List.empty).flatMap { key =>
      dataMapping.hcursor.downField(key).as[dataMappingMetadata].toOption.map(key -> _)
    }.toMap

    println(fieldMetadataMap)


    // Example: Print the metadata for a specific field (replace "SURVEYId" with your desired key)
    //    fieldMetadataMap.get("SURVEYId") match {
    //      case Some(metadata) =>
    //        println("Type: " + metadata.`type`)
    //        println("Key: " + metadata.key)
    //        println("Nullable: " + metadata.nullable)
    //      case None =>
    //        println("Field not found")
    //    }

    val fieldValues = new JHashMap[String, Any]()
    println(fieldValues)
    // Pattern match and call appropriate methods based on the field's type
    fieldMetadataMap.foreach {
      case (fieldName, metadata) =>
        println(s"Field: $fieldName")
        println(s"Type: ${metadata.`type`}")
        println(s"Key: ${metadata.key}")
        println(s"Nullable: ${metadata.nullable}")

        metadata.`type` match {
          case "String" =>
            val result = getString(metadata.`key`, jsonData)
            if (!metadata.nullable && (result == null || result.isEmpty)) {
              throw new Exception("Field cannot be null or empty")
            }
            println(s"String Value: $result")
            fieldValues.put(fieldName, result) // Store the result in the HashMap

          case "Object" =>
            val result = getObject(metadata.`key`, jsonData)
            if (!metadata.nullable && (result == null || result.isEmpty)) {
              throw new Exception("Field cannot be null or empty")
            }
            println(s"String Value: $result")
            fieldValues.put(fieldName, result) // Stor

          case _ =>
            // Handle other types or provide an error message
            println("Unsupported data type")
        }
        println()
    }

    fieldValues.asScala.foreach {
      case (fieldName, result) =>
        println(s"Field: $fieldName, Value: $result")
    }
    println(fieldValues)
  }


}






