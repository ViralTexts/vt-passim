name := "vtpassim"

version := "0.1.0"

scalaVersion := "2.12.10"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.0"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.9.0"

libraryDependencies += "org.kamranzafar" % "jtar" % "2.2"

resolvers += Resolver.sonatypeRepo("public")

lazy val root = (project in file(".")).
   enablePlugins(BuildInfoPlugin).
   settings(
     buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
     buildInfoPackage := "vtpassim"
   )
