package springnz.sparkplug.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest._
import springnz.elasticsearch.server.{ ESServer, ESServerConfig }
import springnz.sparkplug.core.SparkOperation
import springnz.sparkplug.elasticsearch.ESExporter.{ ESExportDetails, ESExporterParams }
import springnz.sparkplug.testkit.SimpleTestContext
import springnz.sparkplug.util.{ Json4sUtil, Logging }

import scala.util.{ Success, Try }

class ESExporterTest extends fixture.WordSpec with ShouldMatchers with Logging {

  val testData = ESExporterTest.testData

  val testDataArray = (parse(testData) match {
    case JArray(sequence) ⇒ sequence
    case _                ⇒ Seq.empty[JValue]
  }) map { jsValue ⇒ pretty(jsValue) }

  override type FixtureParam = ESServer
  val port = 9250

  override def withFixture(test: OneArgTest) = {
    val server = new ESServer(ESServerConfig("test-cluster", httpPort = Some(port)))

    try {
      server.start()
      withFixture(test.toNoArgTest(server)) // "loan" the fixture to the test
    } finally
      server.stop() // clean up the fixture

  }

  "ElasticSearchExporter" should {
    "Save to ES" in { server ⇒
      val context = new SimpleTestContext("ESExporterTest")
      val operation = SparkOperation { ctx ⇒
        val rdd = ctx.parallelize(testDataArray)
        rdd.map { jsonString ⇒
          import Json4sUtil._
          val parsedJson: JValue = parse(jsonString)
          val unWrappedJson = parsedJson.toMap()
          unWrappedJson.asInstanceOf[Map[String, Any]]
        }
      }

      val mappedResult: SparkOperation[(RDD[Map[String, Any]], Try[ESExportDetails])] = for {
        rdd ← operation
        exportOp ← ESExporter[String, Any]("estestindex", "estesttype", ESExporterParams(port = Some(port), idField = "guid"))(rdd)
      } yield exportOp

      val verify = SparkOperation { ctx ⇒
        ctx.esJsonRDD("estestindex/estesttype", Map("es.port" -> port.toString))
      }

      val verifiedResult = for {
        result ← mappedResult
        (rddOfMaps, tryUnit) = result
        verifiedRdd ← verify
      } yield (rddOfMaps.collect(), tryUnit, verifiedRdd.collect())

      log.info("Executing Spark process to run and insert...")
      val result = context.execute(verifiedResult).get

      result._2 shouldBe a[Success[_]]
      val mapArray: Array[Map[String, Any]] = result._1
      mapArray.length shouldBe 5

      val verifiedOutput = result._3
      import Json4sUtil._
      val verifiedJson = verifiedOutput map {
        case (_, output) ⇒ parse(output).toMap().asInstanceOf[Map[String, Any]]
      }

      verifiedJson.map(_.size).head shouldBe 21
      val names = verifiedJson.map(_("name"))
      names.toSet shouldBe Set("Griffin Cole", "Puckett Chen", "Maritza Yang", "Joy Shepherd", "Contreras Sampson")
    }
  }
}

object ESExporterTest {
  val testData =
    """
      |
      |
      |
      |[
      |  {
      |    "index": 0,
      |    "guid": "4662b3a0-2217-4208-bc7a-e9abfb2f80e1",
      |    "isActive": false,
      |    "balance": "$1,682.91",
      |    "picture": "http://placehold.it/32x32",
      |    "age": 25,
      |    "eyeColor": "green",
      |    "name": "Maritza Yang",
      |    "gender": "female",
      |    "company": "APEXTRI",
      |    "email": "maritzayang@apextri.com",
      |    "phone": "+1 (941) 480-3364",
      |    "address": "814 Elmwood Avenue, Wilsonia, Federated States Of Micronesia, 3512",
      |    "about": "Dolor pariatur officia sint ipsum fugiat sunt excepteur sunt enim. Ad duis dolor ullamco quis voluptate enim ipsum adipisicing in. Quis exercitation ullamco duis ipsum pariatur elit id magna. Nulla adipisicing nisi esse sint ex labore occaecat minim deserunt non ea fugiat aliquip.\r\n",
      |    "registered": "2015-03-24T02:58:32 -13:00",
      |    "latitude": 21.30134,
      |    "longitude": 74.58787,
      |    "tags": [
      |      "non",
      |      "mollit",
      |      "labore",
      |      "irure",
      |      "do",
      |      "laborum",
      |      "dolore"
      |    ],
      |    "friends": [
      |      {
      |        "id": 0,
      |        "name": "Valentine Stein"
      |      },
      |      {
      |        "id": 1,
      |        "name": "Mccoy Rosa"
      |      },
      |      {
      |        "id": 2,
      |        "name": "Shirley Knight"
      |      }
      |    ],
      |    "greeting": "Hello, Maritza Yang! You have 7 unread messages.",
      |    "favoriteFruit": "banana"
      |  },
      |  {
      |    "index": 1,
      |    "guid": "fc023297-07cd-4f84-aaab-8c7ee28138c5",
      |    "isActive": true,
      |    "balance": "$1,547.55",
      |    "picture": "http://placehold.it/32x32",
      |    "age": 39,
      |    "eyeColor": "green",
      |    "name": "Puckett Chen",
      |    "gender": "male",
      |    "company": "EBIDCO",
      |    "email": "puckettchen@ebidco.com",
      |    "phone": "+1 (912) 497-2862",
      |    "address": "152 Randolph Street, Sterling, Colorado, 2224",
      |    "about": "Irure et enim dolore ea ad non reprehenderit quis aute exercitation Lorem ad ad aute. Elit officia Lorem est ea. Et aliquip ut ipsum eiusmod mollit ex non esse minim. Laborum irure minim duis qui reprehenderit laborum do nisi ipsum. Voluptate eiusmod sunt ex deserunt exercitation minim irure nulla. Ipsum adipisicing excepteur enim ex ut deserunt deserunt deserunt sit excepteur exercitation. Adipisicing veniam exercitation fugiat do cupidatat nisi eiusmod dolor.\r\n",
      |    "registered": "2015-07-04T10:35:15 -12:00",
      |    "latitude": -72.180266,
      |    "longitude": 144.711888,
      |    "tags": [
      |      "eu",
      |      "sunt",
      |      "excepteur",
      |      "adipisicing",
      |      "occaecat",
      |      "non",
      |      "dolore"
      |    ],
      |    "friends": [
      |      {
      |        "id": 0,
      |        "name": "Maxine Mills"
      |      },
      |      {
      |        "id": 1,
      |        "name": "Turner Owen"
      |      },
      |      {
      |        "id": 2,
      |        "name": "Sheri Donovan"
      |      }
      |    ],
      |    "greeting": "Hello, Puckett Chen! You have 10 unread messages.",
      |    "favoriteFruit": "apple"
      |  },
      |  {
      |    "index": 2,
      |    "guid": "c2a2eb91-031e-448f-85d2-8c6e0f9ce472",
      |    "isActive": true,
      |    "balance": "$3,369.54",
      |    "picture": "http://placehold.it/32x32",
      |    "age": 40,
      |    "eyeColor": "green",
      |    "name": "Joy Shepherd",
      |    "gender": "female",
      |    "company": "SULTRAX",
      |    "email": "joyshepherd@sultrax.com",
      |    "phone": "+1 (867) 438-2007",
      |    "address": "667 Carlton Avenue, Jacksonwald, Maine, 557",
      |    "about": "Amet ex veniam qui ad tempor nisi exercitation esse tempor anim. Voluptate excepteur officia deserunt est consequat aliqua culpa. Ipsum magna pariatur tempor consequat non in excepteur. Irure enim nostrud aliqua culpa mollit eiusmod. Ullamco irure qui minim esse ad magna mollit minim. Reprehenderit quis reprehenderit cupidatat incididunt.\r\n",
      |    "registered": "2015-10-29T09:36:30 -13:00",
      |    "latitude": -46.638031,
      |    "longitude": 10.086202,
      |    "tags": [
      |      "culpa",
      |      "quis",
      |      "cillum",
      |      "cillum",
      |      "consectetur",
      |      "nostrud",
      |      "voluptate"
      |    ],
      |    "friends": [
      |      {
      |        "id": 0,
      |        "name": "Nixon Myers"
      |      },
      |      {
      |        "id": 1,
      |        "name": "Lydia Blankenship"
      |      },
      |      {
      |        "id": 2,
      |        "name": "Stevenson Burns"
      |      }
      |    ],
      |    "greeting": "Hello, Joy Shepherd! You have 10 unread messages.",
      |    "favoriteFruit": "apple"
      |  },
      |  {
      |    "index": 3,
      |    "guid": "fdb3aab7-51b0-436b-a369-7a396156d22c",
      |    "isActive": true,
      |    "balance": "$2,686.13",
      |    "picture": "http://placehold.it/32x32",
      |    "age": 36,
      |    "eyeColor": "green",
      |    "name": "Griffin Cole",
      |    "gender": "male",
      |    "company": "ACCRUEX",
      |    "email": "griffincole@accruex.com",
      |    "phone": "+1 (937) 591-3693",
      |    "address": "141 Bartlett Street, Roulette, Illinois, 4121",
      |    "about": "Cillum consectetur Lorem anim commodo proident qui. Fugiat deserunt cillum cillum cupidatat adipisicing occaecat nisi exercitation. Nostrud quis sint nulla nostrud fugiat sint ea cillum. Dolore minim pariatur duis non anim deserunt. Et id tempor velit reprehenderit ad est culpa enim laborum do magna esse id incididunt.\r\n",
      |    "registered": "2014-05-25T06:09:50 -12:00",
      |    "latitude": -31.950837,
      |    "longitude": 48.566305,
      |    "tags": [
      |      "et",
      |      "ex",
      |      "aute",
      |      "minim",
      |      "do",
      |      "sit",
      |      "non"
      |    ],
      |    "friends": [
      |      {
      |        "id": 0,
      |        "name": "Terra Nguyen"
      |      },
      |      {
      |        "id": 1,
      |        "name": "Yesenia Knowles"
      |      },
      |      {
      |        "id": 2,
      |        "name": "Dana Bolton"
      |      }
      |    ],
      |    "greeting": "Hello, Griffin Cole! You have 6 unread messages.",
      |    "favoriteFruit": "apple"
      |  },
      |  {
      |    "index": 4,
      |    "guid": "e039170f-4d8d-42dd-a8b1-ab792b6f3f96",
      |    "isActive": true,
      |    "balance": "$2,702.73",
      |    "picture": "http://placehold.it/32x32",
      |    "age": 27,
      |    "eyeColor": "green",
      |    "name": "Contreras Sampson",
      |    "gender": "male",
      |    "company": "EMTRAK",
      |    "email": "contrerassampson@emtrak.com",
      |    "phone": "+1 (926) 434-3924",
      |    "address": "331 Guider Avenue, Chesapeake, New York, 9904",
      |    "about": "Proident labore amet ex proident deserunt et dolor pariatur. Labore aute commodo eu in mollit qui do occaecat in nisi. Quis commodo non nisi nostrud aute cupidatat commodo duis excepteur ullamco ullamco tempor exercitation.\r\n",
      |    "registered": "2014-07-18T11:42:34 -12:00",
      |    "latitude": 62.918606,
      |    "longitude": -121.406512,
      |    "tags": [
      |      "Lorem",
      |      "quis",
      |      "adipisicing",
      |      "Lorem",
      |      "in",
      |      "sunt",
      |      "qui"
      |    ],
      |    "friends": [
      |      {
      |        "id": 0,
      |        "name": "Rena Wyatt"
      |      },
      |      {
      |        "id": 1,
      |        "name": "Velazquez Pugh"
      |      },
      |      {
      |        "id": 2,
      |        "name": "Lorena Dyer"
      |      }
      |    ],
      |    "greeting": "Hello, Contreras Sampson! You have 6 unread messages.",
      |    "favoriteFruit": "apple"
      |  }
      |]
    """.stripMargin
}
