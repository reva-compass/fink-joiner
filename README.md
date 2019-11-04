# Flink Joiner POC

## Install flink
   ```bash
    wget http://apache.mirrors.ionfish.org/flink/flink-1.8.0/flink-1.8.0-bin-scala_2.11.tgz
    tar xzf flink-*.tgz
    cd flink-1.8.0
   ```

## Start Flink (locally)

   ```bash
   <PATH_TO_FLINK>/flink-1.8.0/bin/start-cluster.sh
   ```
## Stop Flink

   ```bash
   <PATH_TO_FLINK>/flink-1.8.0/bin/stop-cluster.sh
   ```

## Build code
- Using sbt to compile/build code. 
- Open sbt shell on IntelliJ and run `assembly` to build the code.

## Run Joiner
- You need to tunnel into kafka in order to access the kafka data.
- Execute `run_code.sh` to run the job. Needed params are passed using this script. Modify params as needed.

## Stop Joiner   
- Execute `stop_code.sh` to stop the job.

## Flink Dashboard

- Access UI @ localhost:8081

## Resources

https://ci.apache.org/projects/flink/flink-docs-stable/tutorials/local_setup.html

https://www.ververica.com/blog/kafka-flink-a-practical-how-to

