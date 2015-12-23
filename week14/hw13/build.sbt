name := "pagerank"
version := "1.0"
scalaVersion := "2.10.5"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.5.2", "org.apache.spark" %% "spark-graphx" % "1.5.2")
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"