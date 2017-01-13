package de.model.mutual.information

import com.google.common.math.DoubleMath
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.Model
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 13/01/17.
  */
class PMIEstimator {

}

case class ToolPMI(tool1: String, tool2: String, pmi: Double)

object PMIEstimatorRunner {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("PMI") {
      session => {
        val model: DataFrame = DataSetCreator.createDataSet(session, "model.matrix.file", Model.schema: _*)


        //        val tool1 = "exists-1"
        //        val tool2 = "exists-2"

        val pairs: List[Seq[String]] = Model.tools.combinations(2).toList

        val pmis: List[ToolPMI] = pairs.map(t => {
          val tool1 = t(0)
          val tool2 = t(1)

          val twoTools = model.select(model.col(tool1), model.col(tool2))

          val firstTool = twoTools
            .filter(row => {
              row.getString(0).toInt == 1
            }).count()

          val secondTool = twoTools
            .filter(row => {
              row.getString(1).toInt == 1
            }).count()

          val cooccuringTools = twoTools
            .filter(row => {
              val firstElement = row.getString(0).toInt == 1
              val secondElement = row.getString(1).toInt == 1
              firstElement && secondElement

            }).count()

          println(s" First Tool [$tool1] found: $firstTool")
          println(s" Second Tool [$tool2] found: $secondTool")
          println(s" Coocurence of tool 1 and tool 2: $cooccuringTools")

          val pmi: Double = Math.log(cooccuringTools.toDouble / (firstTool.toDouble * secondTool.toDouble))
          val pmiWithLog2 = DoubleMath.log2(cooccuringTools.toDouble / (firstTool.toDouble * secondTool.toDouble))

          println(s" PMI of two tools: $pmi")
          println(s" LOG2 PMI of two tools: $pmiWithLog2")
          println(s" ------******------ ")

          ToolPMI(tool1, tool2, pmi)

        })

        pmis.sortWith((t1, t2) => {
          t1.pmi < t2.pmi
        }).foreach(println)
      }
    }
  }
}
