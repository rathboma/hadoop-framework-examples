package test.scala.com.matthewrathbone.scalding
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.WordSpec
import org.scalatest.Matchers
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import com.twitter.scalding.TypedTsv

@RunWith(classOf[JUnitRunner])
class MainTypedFunctionJoinText extends WordSpec with Matchers{
  "Our job" should {
    JobTest(new main.scala.com.matthewrathbone.scalding.Main(_))
      .arg("input1", "usersInput")
      .arg("input2", "transactionsInput")
      .arg("output", "outputFile")
      .source(TextLine("usersInput"), List((0, "1	matthew@test.com	EN	US"), (1, "2	matthew@test2.com	EN	GB"), (2, "3	matthew@test3.com	FR	FR")))
      .source(TextLine("transactionsInput"), List((0, "1	1	1	300	a jumper"), (1, "2	1	2	300	a jumper"), (2, "3	1	2	300	a jumper"), (3, "4	2	3	100	a rubber chicken"), (4, "5	1	3	300	a jumper")))
      .sink[(Long, Long)](TypedTsv[(Long, Long)]("outputFile")){buff =>
        val outMap = buff.toMap
      	"output should contain 2 lines " in {
        	buff.length should be (2)
        }
        "values are " in {
        	outMap(1) shouldBe 3
            outMap(2) shouldBe 1
      	}
      }
      .run
      .finish
  }
}
