package de.evaluation.util

import java.util.concurrent.TimeUnit

/**
  * Created by visenger on 11/04/17.
  */
object Timer {

  def measureRuntime(f: () => Unit) = {
    val start = System.nanoTime()
    f()
    val finish = System.nanoTime()
    val durationInMillisec = TimeUnit.NANOSECONDS.toMillis(finish - start)
    println(s"Duration time: $durationInMillisec ms")
  }

}
