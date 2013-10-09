package com.matthewrathbone.example

import org.junit.Test
import com.matthewrathbone.helpers.MapReduceTester
import com.matthewrathbone.helpers.MapReduceTester.TestJob
import com.matthewrathbone.helpers.MapReduceTester.JobArgs
import org.apache.hadoop.fs.Path
import java.util.List
import java.util.Arrays
import org.junit.Assert
import org.hamcrest.core.Is.is


class TestAllTheThings {

  @Test
  def integrationTest() {

    val runner = new MapReduceTester()

    val job: TestJob = new TestJob {
      override def run(args: JobArgs) {
        val usersString = "1\tmatthew@test.com\tEN\tUS\n" +
          "2\tmatthew@test2.com\tEN\tGB\n" +
          "3\tmatthew@test3.com\tFR\tFR"

        val transactionString = 
            "1\t1\t1\t300\ta jumper\n" +
            "2\t1\t2\t300\ta jumper\n" +
            "3\t1\t2\t300\ta jumper\n" +
            "4\t2\t3\t100\ta rubber chicken\n" +
            "5\t1\t3\t300\ta jumper"

        val transactions = runner.registerString(transactionString)
        val users = runner.registerString(usersString)
        val output = runner.registerDirectory(false)
        Example.main(Array(users.toString, transactions.toString, output.toString))
        val results = runner.collectStrings(output)
        Assert.assertEquals("number of results is correct", results.size(), 2)
        val expected = Arrays.asList("(1,3)", "(2,1)")
        Assert.assertThat(results, is(expected))

      }
    }
    runner.run(job)
    
  }


}
