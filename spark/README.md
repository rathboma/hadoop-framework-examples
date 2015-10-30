
## Compiling
``` bash
mvn compile
```

## Testing
``` bash
mvn test
```

## Running
``` bash
mvn assembly:single

``` bash
/usr/bin/spark-submit --class main.java.com.matthewrathbone.sparktest.SparkJoins --master local ./target/spark-example-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/transactions_test.txt /path/to/users_test.txt /path/to/output_folder
