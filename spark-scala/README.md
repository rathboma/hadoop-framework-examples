
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
/usr/bin/spark-submit --class main.scala.com.matthewrathbone.spark.Main --master local ./target/scala-spark-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/transactions_test.txt /path/to/users_test.txt /path/to/output_folder
