mvn clean package
spark-submit --class main.Main --master spark://10.129.149.40:7077   --conf spark.executor.cores=2 --conf spark.cores.max=8 --conf spark.executor.memory=8g --jars target/CachingSystem-1.0-SNAPSHOT.jar,target/dependency/* --deploy-mode client target/CachingSystem-1.0-SNAPSHOT-jar-with-dependencies.jar ./cachesystem.properties

