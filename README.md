## Setup and run

### Build with Maven
In flink-exercise
```
mvn clean package
```

### Prepare Flink
Assuming flink-1.20.0 binary has been downloaded and extracted into the repo.

In `flink-1.20.0/conf/config.yaml`
Edit numberOfTaskSlots to allow for increased parallelism.

### Start Flink
```
flink-1.20.0/bin/start-cluster.sh
```

### Run application
```
flink-1.20.0/bin/flink run flink-exercise/target/flink-exercise-1.0-SNAPSHOT.jar --parallelism 4 --out "$PWD"/output
```
This will submit the compiled application to the local cluster with parallelism 4. The result should end up in the output directory.

