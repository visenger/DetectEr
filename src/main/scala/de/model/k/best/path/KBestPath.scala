package de.model.k.best.path

import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.Model
import org.apache.spark.sql.DataFrame

import scala.annotation.tailrec
import scala.collection.immutable.Range.Inclusive

/**
  * inspired by Viterbi algorithm K-Best-Path
  */
class KBestPath {

}

case class Trelli(tool1: String, tool2: String, total: Long) {
  override def toString: String = s" $tool1 -> $tool2[$total]"
}


object KBestPathRunner {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("K-BEST-PATH") {
      session => {
        val model: DataFrame = DataSetCreator.createDataSet(session, "model.matrix.file", Model.schema: _*)
        val sampleSize: Long = model.count()

        val tools = Model.tools

        val initialRun: Seq[Trelli] = tools.map(tool => {
          val singleToolPerformance: Long = model
            .select(tool)
            .filter(_.getString(0).toInt == 1)
            .count()
          Trelli("_", tool, singleToolPerformance)
        })
        val initMaxTrelli: Trelli = initialRun.reduce((t1, t2) => {
          val maxCount = Math.max(t1.total, t2.total)
          val maxTool = Seq(t1, t2).filter(_.total == maxCount).head
          maxTool
        })

        val nextTrelli = initMaxTrelli :: getAllTrellis(model, tools, initMaxTrelli)

        nextTrelli.foreach(println)


      }
    }
  }


  private def getAllTrellis(model: DataFrame, tools: Seq[String], initMaxTrelli: Trelli): List[Trelli] = {

    def getTrellis(model: DataFrame, tools: Seq[String], initMaxTrelli: Trelli): List[Trelli] = {

      tools match {
        case Nil => List()
        case Seq(x) => List()
        case _ => {
          val maxTrelli = getMaxTrelli(model, tools, initMaxTrelli)
          println(maxTrelli.toString)
          val remindedTools: Seq[String] = tools.partition(_.equalsIgnoreCase(initMaxTrelli.tool2))._2
          maxTrelli :: getTrellis(model, remindedTools, maxTrelli)
        }
      }
    }

    getTrellis(model, tools, initMaxTrelli)
  }

  private def getMaxTrelli(model: DataFrame, tools: Seq[String], initMaxTrelli: Trelli): Trelli = {
    val sampleSize: Long = model.count()
    val startingTool = initMaxTrelli.tool2
    val partition: (Seq[String], Seq[String]) = tools.partition(_.equalsIgnoreCase(startingTool))

    val remindedTools: Seq[String] = partition._2
    println(s""" input tools: ${tools.mkString(", ")}""")
    println(s"""reminded tools: ${remindedTools.mkString(", ")}""")

    val toolsPairs: List[(String, String)] = remindedTools.map(t => (startingTool, t)).toList

    val detectedErrors: List[(String, String, Long)] = toolsPairs.map(tools => {
      val nothingFound: Long = model.select(tools._1, tools._2)
        .filter(row => {
          val noResultFirstTool = row.getString(0).toInt == 0
          val noResultSecondTool = row.getString(1).toInt == 0
          noResultFirstTool && noResultSecondTool
        }).count()
      val found: Long = sampleSize - nothingFound
      (tools._1, tools._2, found)
    })

    val maxTrelli: (String, String, Long) = detectedErrors.reduce(
      (a, b) => {
        val maxVal = Math.max(a._3, b._3)
        val trelli: (String, String, Long) = Seq(a, b).filter(_._3 == maxVal).head
        trelli
      }
    )
    Trelli(maxTrelli._1, maxTrelli._2, maxTrelli._3)
  }

}

object KBestPathPlayground {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("K-BEST-PATH") {
      session => {
        val model: DataFrame = DataSetCreator.createDataSet(session, "model.matrix.file", Model.schema: _*)
        val sampleSize: Long = model.count()

        val dim = Model.tools.size
        val toolsWithIndex = Model.toolsWithIndex

        val transitionMatrix: Array[Array[Trelli]] = Array.ofDim[Trelli](dim, dim)

        val indicies: Inclusive = 0 to dim - 1

        indicies.foreach(i => {
          indicies.foreach(f = j => {
            val value: Trelli = i == j match {
              case true => Trelli("-", "-", 0L)
              case false => {
                //todo: extract complex method of computation

                val toolsPerf: Trelli = j == 0 match {
                  case true => {
                    //compute just for i
                    val tool: String = toolsWithIndex.get(i + 1).get
                    val singleToolPerformance: Long = model
                      .select(tool)
                      .filter(_.getString(0).toInt == 1)
                      .count()
                    Trelli("-", tool, singleToolPerformance)
                  }
                  case false => {
                    //i -> row num
                    //j -> column num

                    val iTh = i + 1
                    //tool-j = i;j
                    val tool1 = toolsWithIndex.get(iTh).get

                    val secondTools = (1 to dim).filterNot(_ == iTh).map(i => toolsWithIndex.get(i).get)
                    val toolsPairs: List[(String, String)] = secondTools.map(tool2 => (tool1, tool2)).toList

                    val detectedErrors: List[(String, String, Long)] = toolsPairs.map(tools => {
                      val nothingFound: Long = model.select(tools._1, tools._2)
                        .filter(row => {
                          val noResultFirstTool = row.getString(0).toInt == 0
                          val noResultSecondTool = row.getString(1).toInt == 0
                          noResultFirstTool && noResultSecondTool
                        }).count()
                      val found: Long = sampleSize - nothingFound
                      (tools._1, tools._2, found)
                    })

                    val maxTrelli: (String, String, Long) = detectedErrors.reduce(
                      (a, b) => {
                        val maxVal = Math.max(a._3, b._3)
                        val trelli: (String, String, Long) = Seq(a, b).filter(_._3 == maxVal).head
                        trelli
                      }
                    )
                    Trelli(maxTrelli._1, maxTrelli._2, maxTrelli._3)


                    //tool-(j-1) = (k)(j-1) where k!=i
                    //max

                    // 1L
                  } // todo: compute max of cost
                }
                toolsPerf
              } //todo: extract complex method of computation
            }
            transitionMatrix(i)(j) = value
          })
        })

        transitionMatrix.foreach(a => println(s"""${a.mkString(" , ")}"""))

      }
    }
  }

}
