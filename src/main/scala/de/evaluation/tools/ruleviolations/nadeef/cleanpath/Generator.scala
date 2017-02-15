package de.evaluation.tools.ruleviolations.nadeef.cleanpath

/**
  * Created by visenger on 22/11/16.
  */
class Generator {

}

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.ConfigFactory

import scala.io.Source

/**
  * Creates JSON data cleaning rules for processing by NADEEF
  * Using functional dependencies form: A,B->C
  */


object NadeefCleanPathRulesGenerator extends App {
  //subject of change:
  val prefix = "hosp"
  val folder = "data.hosp.fds.rules.folder"
  val file = "data.hosp.fds.rules.file"
  val noisy = "data.hosp.dirty.10k"

  //general:
  val config = ConfigFactory.load()
  val dcPath: String = config.getString(folder)
  val dcFile: String = config.getString(file)
  val noisyData = config.getString(noisy)


  val rawRules: List[String] = Source.fromFile(s"$dcPath/$dcFile").getLines().filterNot(_.isEmpty).toList


  val preparedDCRules: List[String] = for (fd <- rawRules) yield {
    val rule: String = parseDCRule(fd)
    rule
  }

  var counter = 0
  val rules: List[String] = for (rule <- preparedDCRules) yield {

    s"""|        {
        |			"name" : "${prefix.toUpperCase}FD${counter += 1; counter}",
        |            "type" : "fd",
        |            "value" : $rule
        |        }""".stripMargin
  }
  val rulesAsJson: String = rules.mkString(",\n")


  val template =
    s"""|{
        |    "source" : {
        |        "type" : "csv",
        |        "file" : ["$noisyData"]
        |    },
        |    "rule" : [
          $rulesAsJson
        | ]
        |}
       """.stripMargin


  private val cleanPath = s"$dcPath/cleanpath.json"
  val path: Path = Paths.get(cleanPath)
  val writer: BufferedWriter = Files.newBufferedWriter(path, StandardCharsets.UTF_8)
  println(s"writing clean path into $cleanPath")
  writer.write(template)
  writer.close()

  println(s"TODO: now, put $cleanPath to NADEEF_HOME")


  def parseDCRule(input: String): String = {
    val Array(lhs, rhs) = input.split("->")
    val lhsFinished: String = prepareDCRulePart(lhs)
    val rhsFinished: String = prepareDCRulePart(rhs)

    val dsRule: String = Seq(lhsFinished, rhsFinished).mkString( s""" [" """, "|", s""" "] """)

    dsRule
  }

  def prepareDCRulePart(input: String): String = {
    val lhsWithoutBorders: String = removeBorder(input)
    val lhsSplitted: Array[String] = removeRulesNumbers(lhsWithoutBorders)
    val lhsFinished: String = lhsSplitted.mkString(",")
    lhsFinished
  }

  def removeRulesNumbers(input: String): Array[String] = {
    input.split(",").map(_.replaceAll("\\d+\\.".r.toString(), ""))
  }

  private def removeBorder(input: String): String = {
    input.filter(_.!=('[')).filter(_.!=(']'))
  }
}


