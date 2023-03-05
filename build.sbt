ThisBuild/ scalaVersion := "2.12.15"

name := "spark-type-safety"

lazy val sparkVersion   = "3.3.2"

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark"  %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark"  %% "spark-sql"  % sparkVersion % Provided,

  // Testing
  "org.scalatest"     %% "scalatest"   % "3.2.10"   % Test,
)
