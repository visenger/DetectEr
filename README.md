# DetectEr
Data Cleaning Tools Orchestration.

The accompanying paper: Visengeriyeva, Larysa, and Ziawasch Abedjan. ["Metadata-driven error detection."](https://www.researchgate.net/publication/326309905_Metadata-driven_error_detection) Proceedings of the 30th International Conference on Scientific and Statistical Database Management. ACM, 2018.
Starting script is [here](https://github.com/visenger/DetectEr/blob/master/src/main/scala/de/experiments/features/error/prediction/ErrorPredictorRunner.scala).
Datasets are available [here](https://github.com/visenger/clean-and-dirty-data).

We developed two approaches for aggregating error detection results and presented them as a part of
a holistic error detection system. These strategies rely on the output of the constituent data cleaning
systems and automatically extracted metadata. Our experiments support our hypotheses, stating that

1. aggregating data cleaning system results can be viewed as a classification task and our strategies
are more accurate than the baseline approaches and individual data cleaning systems;
2. augmenting system features with automatically generated metadata information will improve
the error predicting outcome and
3. adding more data cleaning system has only marginal impact if the results strongly overlap with other systems.
