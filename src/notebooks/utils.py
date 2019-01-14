import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from sklearn.metrics import confusion_matrix
from numpy import linalg as la


def compute_hadamard(ms, ns):
    uu = np.asarray(ms).flatten()
    gg = np.asarray(ns).flatten()
    w_hat = list(map( lambda i, j: 0 if (j==0) else i/j, uu, gg))
    return w_hat


def f1(confMat):
    tn=confMat[0][0]
    fp=confMat[0][1]
    fn=confMat[1][0]
    tp=confMat[1][1]
    precision = tp/(tp+fp)
    recall = tp/(tp+fn)
    f_1= 2*precision*recall/(precision+recall)
    print('Precision: %0.4f, Recall: %0.4f, F-1: %0.4f'% (precision, recall, f_1))