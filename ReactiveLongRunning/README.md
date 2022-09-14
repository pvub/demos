# Handle long running operations
Demo application to depict an application design that incorporates WebSockets to enable publishing real-time updates to long running operations.

## Build
```
mvn clean package
```

## Run
```
java -Dlog4j.configurationFile=log4j2-api.xml -Dreactiveapi.config=conf/config.json -jar target/ReactiveLongRunning-1.0-SNAPSHOT-fat.jar
```

## Use
Load http://localhost:8080/static/index.html
Specify a Transaction label and a Customer label, and Submit.
