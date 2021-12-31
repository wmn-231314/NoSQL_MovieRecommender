# coding=utf-8
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model
from saving_model import SavingRanking

df = pd.read_csv('ratings_train.csv')
test_ratio = 0.5
np.random.seed(40)

shuffled_indices = np.random.permutation(len(df))
test_set_size = int(len(df) * test_ratio)
test_indices = shuffled_indices[:test_set_size]
train_indices = shuffled_indices[test_set_size:]
train = df.iloc[:test_set_size]
test = df.iloc[test_set_size:]
print('finish shuffle!')
train.to_csv('final_train.csv',index=False)
test.to_csv('final_test.csv',index=False)
print('finish saving!')
