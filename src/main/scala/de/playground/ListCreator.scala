package de.playground

import java.io.{File, PrintWriter}

/**
  * Created by visenger on 27.06.17.
  */
object ListCreator {


  def main(args: Array[String]): Unit = {
    val a: List[String] = (1 to 3230).map(i => s"a-$i").toList
    val b: List[String] = (1 to 549).map(i => s"b-$i").toList
    val c: List[String] = (1 to 888).map(i => s"c-$i").toList

    val ab: List[String] = (1 to 414).map(i => s"ab-$i").toList
    val ac: List[String] = (1 to 462).map(i => s"ac-$i").toList
    val bc: List[String] = (1 to 4992).map(i => s"bc-$i").toList

    val abc: List[String] = (1 to 236020).map(i => s"abc-$i").toList

    val nb = a ++ ab ++ ac ++ abc
    val nn = b ++ ab ++ bc ++ abc
    val dt = c ++ bc ++ ac ++ abc

    createFileFor(nb, "naive-bayes")
    createFileFor(nn, "neural-networks")
    createFileFor(dt, "decision-tree")
  }

  def createFileFor(list: List[String], name: String): Unit = {


    val writer = new PrintWriter(new File(s"/Users/visenger/research/datasets/EXPERIMENTS/classifier-results/$name.txt"))
    list.foreach(item => {
      writer.write(s"$item\n")
    })
    writer.close()
  }

}
