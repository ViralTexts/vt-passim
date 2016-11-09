name := "vtpassim"

version := "0.1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.0"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.6.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models"

libraryDependencies += "org.kamranzafar" % "jtar" % "2.2"

resolvers += Resolver.sonatypeRepo("public")

lazy val root = (project in file(".")).
   enablePlugins(BuildInfoPlugin).
   settings(
     buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
     buildInfoPackage := "vtpassim"
   )
