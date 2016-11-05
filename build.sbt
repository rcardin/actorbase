name := "actorbase"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.4.12",
  "org.scalactic"     %% "scalactic"    % "3.0.0",
  "org.scalatest"     %% "scalatest"    % "3.0.0"   % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.12"  % "test"
)