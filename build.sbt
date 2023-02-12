// *****************************************************************************
// Build settings
// *****************************************************************************

inThisBuild(
  Seq(
    organization     := "example.com",
    organizationName := "ksilin",
    startYear        := Some(2023),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := "2.13.8",
    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked",
      "-encoding",
      "UTF-8",
      "-Ywarn-unused:imports",
      // "-Xfatal-warnings",
    ),
    scalafmtOnCompile := true,
    dynverSeparator   := "_", // the default `+` is not compatible with docker tags
    resolvers ++= Seq(
      "confluent" at "https://packages.confluent.io/maven",
      "ksqlDb" at "https://ksqldb-maven.s3.amazonaws.com/maven",
      Resolver.sonatypeRepo("releases"),
      Resolver.bintrayRepo("wolfendale", "maven"),
      Resolver.mavenLocal,
      "jitpack" at "https://jitpack.io"
    ),
    Test / fork := true, // required for setting env vars
  )
)

// *****************************************************************************
// Projects
// *****************************************************************************

lazy val parallel_consumer_scala = project
  .in(file("."))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      library.clients,
      library.kafka,
      library.parallelConsumer,
      library.progressbar,
      library.betterFiles,
      library.config,
      library.gson,
      library.circe,
      library.circeGeneric,
      library.circeParser,
      library.circeKafka,
      library.airframeLog,
      library.logbackClassic,
      library.logbackCore,
      library.scalatest % Test
    ),
  )

// *****************************************************************************
// Project settings
// *****************************************************************************

lazy val commonSettings =
  Seq(
    // Also (automatically) format build definition together with sources
    Compile / scalafmt := {
      val _ = (Compile / scalafmtSbt).value
      (Compile / scalafmt).value
    },
  )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {
    object Version {
      val kafka            = "3.4.0"
      val parallelConsumer = "0.5.2.4"
      val progressbar      = "0.9.3"
      val betterFiles      = "3.9.2"
      val airframeLog      = "23.2.4"
      val config           = "1.4.2"
      val gson             = "2.10.1"
      val circeKafka       = "3.3.1"
      val circe            = "0.14.4"
      val logback          = "1.3.4"
      val scalatest        = "3.2.15"
    }

    val clients      = "org.apache.kafka"      % "kafka-clients" % Version.kafka
    val kafka        = "org.apache.kafka"     %% "kafka"         % Version.kafka
    val betterFiles  = "com.github.pathikrit" %% "better-files"  % Version.betterFiles
    val airframeLog  = "org.wvlet.airframe"   %% "airframe-log"  % Version.airframeLog
    val config       = "com.typesafe"          % "config"        % Version.config
    val gson         = "com.google.code.gson"  % "gson"          % Version.gson
    val circeKafka   = "com.nequissimus"      %% "circe-kafka"   % Version.circeKafka
    val circe        = "io.circe"             %% "circe-core"    % Version.circe
    val circeGeneric = "io.circe"             %% "circe-generic" % Version.circe
    val circeParser  = "io.circe"             %% "circe-parser"  % Version.circe
    val parallelConsumer =
      "io.confluent.parallelconsumer" % "parallel-consumer-core" % Version.parallelConsumer
    val progressbar    = "me.tongfei"     % "progressbar"     % Version.progressbar
    val logbackCore    = "ch.qos.logback" % "logback-core"    % Version.logback
    val logbackClassic = "ch.qos.logback" % "logback-classic" % Version.logback
    val scalatest      = "org.scalatest" %% "scalatest"       % Version.scalatest
  }
