
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
mvn assembly:single # this will bundle dependencies, we actually don't have any, but good to set up.
hadoop jar target/scala-scalding-1.0-SNAPSHOT-jar-with-dependencies.jar com.twitter.scalding.Tool main.scala.com.matthewrathbone.scalding.Main --hdfs --input1 /path/to/input1 --input2 /path/to/input2 --output /path/to/output
```
