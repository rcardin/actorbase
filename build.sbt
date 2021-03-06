name := "actorbase"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-actor"    % "2.5.8",
  "org.scalactic"      %% "scalactic"     % "3.0.0",
  "org.scalatest"      %% "scalatest"     % "3.0.0"   % "test",
  "com.typesafe.akka"  %% "akka-testkit"  % "2.5.8"  % "test",
  "org.apache.commons" %  "commons-lang3" % "3.5"     % "test"
)

lazy val messages = project in file("actorbase-api-messages")
lazy val actorbase = (project in file("."))
  .aggregate(messages)
  .dependsOn(messages)
