package de.model.mutual.information

/**
  * Created by visenger on 23/02/17.
  */
object PMIRunAll {

  def main(args: Array[String]): Unit = {
    PMIEstimatorRunner.run()
    HospPMIEstimatorRunner.run()
    SalariesPMIEstimatorRunner.run()
  }

}
