ThisBuild / version := "0.1.0-SNAPSHOT"
name := "cdc_example"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.18"
libraryDependencies += "io.delta" %% "delta-core" % "2.2.0"