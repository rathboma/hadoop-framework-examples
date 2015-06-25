
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
hadoop jar target/java-cascading-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/users /path/to/transactions /path/to/output
```