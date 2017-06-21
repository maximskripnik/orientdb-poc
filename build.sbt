name := "orientdb-async-poc"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "com.michaelpollmeier" %% "gremlin-scala" % "3.2.4.15",
  "com.michaelpollmeier" % "orientdb-gremlin" % "3.2.3.0",
  "com.typesafe" % "config" % "1.3.1"
)

fork := true //Mandatory. Otherwise it complains about javascript(?). See https://github.com/orientechnologies/orientdb/issues/5274