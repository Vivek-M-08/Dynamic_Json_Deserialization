//import play.api.libs.json._
//import SurveySchema._
//
//import scala.io.Source
//
//case class Person(name: String, age: Int)
//
//object Main extends App {
//  implicit val personReads: Reads[Person] = Json.reads[Person]
//
//  val filePath = "/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/event.json"
//
//  // Read JSON content from the file
//  val jsonContent = Source.fromFile(filePath).getLines.mkString
//
//  println(jsonContent)
//  val jsonData = Json.parse(jsonContent)
//
//  val jsonSchema = jsonData.validate[SurveySchema] //jsonContent
//  println(jsonData)
//
//  println(SurveySchema)
//
//  //val readJson = Json.reads."/Users/user/Desktop/Simple_Json_Poc/src/main/scala/com/example/event.json")
////  val jsonData = Json.parse("""{"name":"John","age":30,"A":{"B":"c"}}""")
////  println(jsonData)
//
//  val result: JsResult[Person] = jsonData.validate[Person]
//  println(result)
//
//  result match {
//    case JsSuccess(person, _) => println(s"Name: ${person.name}, Age: ${person.age}")
//    case JsError(errors) => println(s"Failed to deserialize JSON: $errors")
//  }
//}
