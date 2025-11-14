// Bu, projemizin Scala ve sbt sürümünü belirler
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0-SNAPSHOT"

// Apache Spark'tan "SQL" (Veritabanı) kütüphanesini ekle
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"

// PostgreSQL veritabanı sürücüsünü ekle
libraryDependencies += "org.postgresql" % "postgresql" % "42.7.3"