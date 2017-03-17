package de.model.logistic.regression

/**
  * Created by visenger on 16/02/17.
  */
case class TestData(totalTest: Long,
                    wrongPrediction: Long,
                    accuracy: Double,
                    precision: Double,
                    recall: Double,
                    f1: Double,
                    info: String = "") {
  override def toString: String = {
    s"TEST DATA INFO: Accuracy: $accuracy, Precision: $precision, Recall: $recall, F1: $f1, totalTest: $totalTest, wrongPrediction: $wrongPrediction"
  }
}
