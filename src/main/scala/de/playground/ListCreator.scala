package de.playground

import java.io.{File, PrintWriter}

import de.model.util.NumbersUtil

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


    val writer = new PrintWriter(new File(s"/Users/visenger/research/datasets/EXPERIMENTS/repair/vienn-viz/$name.txt"))
    list.foreach(item => {
      writer.write(s"$item\n")
    })
    writer.close()
  }

}

object SetsIntersection extends App {
  /*
  FD-first-element (a), hist (b), fds-all (c), all-clean (d).
((0.0,0.0,1.0,1.0),39734)
((1.0,0.0,1.0,1.0),19717)
((1.0,0.0,0.0,1.0),2226)
((1.0,1.0,1.0,1.0),2102)
((1.0,0.0,0.0,0.0),2059)
((0.0,1.0,1.0,1.0),626)
((0.0,0.0,0.0,1.0),4549)
((1.0,1.0,0.0,1.0),242)
((0.0,1.0,0.0,1.0),64)
  * */


  val a: List[String] = (1 to 2059).map(i => s"a-$i").toList
  //  val b: List[String] = (1 to 549).map(i => s"b-$i").toList
  // val c: List[String] = (1 to 888).map(i => s"c-$i").toList
  val d: List[String] = (1 to 4549).map(i => s"d-$i").toList

  val cd: List[String] = (1 to 39734).map(i => s"cd-$i").toList
  val ad: List[String] = (1 to 226).map(i => s"ad-$i").toList
  val bd: List[String] = (1 to 64).map(i => s"bd-$i").toList

  val acd: List[String] = (1 to 19717).map(i => s"acd-$i").toList
  val bcd: List[String] = (1 to 626).map(i => s"bcd-$i").toList
  val abd: List[String] = (1 to 242).map(i => s"abd-$i").toList

  val abcd: List[String] = (1 to 2102).map(i => s"abcd-$i").toList


  val fd_first_elment = a ++ ad ++ acd ++ abd ++ abcd
  val hist = bd ++ abd ++ bcd ++ abcd
  val fd_all = cd ++ acd ++ bcd ++ abcd
  val clean_all = d ++ cd ++ ad ++ bd ++ acd ++ abd ++ abcd

  val total = a ++ d ++ cd ++ ad ++ bd ++ acd ++ bcd ++ abd ++ abcd
  val totalSize = total.size

  //FD-first-element (a), hist (b), fds-all (c), all-clean (d).
  println(s" prob FD-first-element =  ${NumbersUtil.round(a.size / totalSize.toDouble, 4)}")
  println(s" prob all-clean = ${NumbersUtil.round(d.size / totalSize.toDouble, 4)}")
  println(s" prob fds-all+all-clean = ${NumbersUtil.round(cd.size / totalSize.toDouble, 4)}")
  println(s" prob FD-first-element + all-clean = ${NumbersUtil.round(ad.size / totalSize.toDouble, 4)}")
  println(s" prob hist+all-clean = ${NumbersUtil.round(bd.size / totalSize.toDouble, 4)}")
  println(s" prob FD-first-element +fds-all+all-clean = ${NumbersUtil.round(acd.size / totalSize.toDouble, 4)}")
  println(s" prob hist + fds-all+all-clean = ${NumbersUtil.round(bcd.size / totalSize.toDouble, 4)}")
  println(s" prob FD-first-element + hist+ all-clean = ${NumbersUtil.round(abd.size / totalSize.toDouble, 4)}")
  println(s" prob FD-first-element + hist+ fds-all+all-clean = ${NumbersUtil.round(abcd.size / totalSize.toDouble, 4)}")

  //  createFileFor(fd_first_elment, "fd-first-element")
  //  createFileFor(hist, "hist")
  //  createFileFor(fd_all, "fd-all")
  //  createFileFor(clean_all, "clean-all")

}
