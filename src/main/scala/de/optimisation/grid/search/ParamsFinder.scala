package de.optimisation.grid.search


class ParamsFinder {

  def find = {
    import sys.process._

    val goldData = "/Users/visenger/research/datasets/BlackOak/Archive/small-groundDB.csv"
   // "/Users/visenger/PycharmProjects/dBoost/dboost"

    val script = "/../../PycharmProjects/dBoost/dboost/dboost-stdin.py"
    val input = "/Users/visenger/research/datasets/BlackOak/Archive/small-inputDB.csv"
    val outputFolder = "/Users/visenger/research/datasets/BlackOak/Archive/outlier-output"

    val strategy = "histogram"
    val param1 = "0.9"
    val param2 = "0.01"

    val cli = s".$script -F ,  --$strategy $param1 $param2 --discretestats 8 2 -d string_case $input"
    val outputFile = s"$outputFolder/out-DBoost-$strategy-$param1-$param2.txt"


    s"$cli" #> s"$outputFile" !


  }


}

object ParamsFinder {
  def main(args: Array[String]): Unit = {
    new ParamsFinder().find
  }
}
