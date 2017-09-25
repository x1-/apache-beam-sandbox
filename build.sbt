organization := "com.inkenkun.x1"

name         := "apache-beam-sandbox"

version      := "0.1"

scalaVersion := "2.12.3"

val beamVersion  = "2.1.0"
val circeVersion = "0.8.0"
val scioVersion  = "0.4.1"

libraryDependencies ++= Seq(
  "com.spotify"         %% "scio-core"                              % scioVersion,
  "io.circe"            %% "circe-core"                             % circeVersion,
  "io.circe"            %% "circe-generic"                          % circeVersion,
  "io.circe"            %% "circe-parser"                           % circeVersion,
  "io.circe"            %% "circe-generic-extras"                   % circeVersion,
  "org.apache.beam"     % "beam-sdks-java-core"                     % beamVersion,
  "org.apache.beam"     % "beam-runners-direct-java"                % beamVersion,
  "org.apache.beam"     % "beam-runners-core-construction-java"     % beamVersion,
  "org.apache.beam"     % "beam-runners-core-java"                  % beamVersion,
  "org.apache.beam"     % "beam-sdks-common-runner-api"             % beamVersion,
  "org.apache.beam"     % "beam-runners-google-cloud-dataflow-java" % beamVersion,
  "org.apache.beam"     % "beam-sdks-java-io-google-cloud-platform" % beamVersion,
  "com.spotify"         %% "scio-test"                              % scioVersion % "test",
  "org.scalatest"       %% "scalatest"                              % "3.0.1" % "test",
  "org.scalacheck"      %% "scalacheck"                             % "1.13.4" % "test"
)
