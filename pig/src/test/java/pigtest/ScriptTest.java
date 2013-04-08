package pigtest;


import org.apache.pig.ExecType;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.junit.Test;

public class ScriptTest {
  // THIS DOES NOT WORK
  @Test
  public void test() throws Exception {
    // output is ignored by pigtest
    String[] args = new String[]{
      "OUTPUT=foo", 
      "USERS=src/test/resources/users.txt",
      "TRANSACTIONS=src/test/resources/transactions.txt"
    };
    String script = "src/main/pig/script.pig";
    
    
    PigTest test = new PigTest(script, args);
    String[] expectedOutput = {"(1,3)", "(2,1)"};
    test.assertOutput("C", expectedOutput);
  }
}
