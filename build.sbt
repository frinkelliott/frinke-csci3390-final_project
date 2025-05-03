// build.sbt
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import sbtassembly.PathList

name := "final_project"
version := "1.0"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % "3.1.2",
  "org.apache.spark" %% "spark-sql"     % "3.1.2",
  "org.apache.spark" %% "spark-graphx"  % "3.1.2",
  "org.jgrapht"      %  "jgrapht-core"  % "1.5.1"
)

// Name the “fat” JAR something recognizable
assemblyJarName in assembly := s"${name.value}-assembly-${version.value}.jar"

// (Optional) If you ever omit --class on spark-submit, this is your entry point
mainClass in assembly := Some("final_project.main")

// Merge rules for conflicting files in META-INF, JAXB, Activation, AOP, etc.
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _ @ _*)                       => MergeStrategy.discard
  case PathList("javax","xml","bind", _ @ _*)             => MergeStrategy.first
  case PathList("jakarta","xml","bind", _ @ _*)           => MergeStrategy.first
  case PathList("com","sun","activation", _ @ _*)         => MergeStrategy.first
  case PathList("javax","activation", _ @ _*)             => MergeStrategy.first
  case PathList("org","aopalliance", _ @ _*)              => MergeStrategy.first
  case PathList("org","glassfish","hk2", _ @ _*)          => MergeStrategy.first
  case _                                                  => MergeStrategy.first
}
