import base64
import os
import sys
import timeit
from collections import defaultdict
import redis
from recsys import create_utility_matrix,svd
from scipy.linalg import sqrtm
from surprise import SVD, Reader
from surprise import Dataset
from surprise.model_selection import cross_validate
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics.pairwise import cosine_similarity

r = redis.Redis(
    host='127.0.0.1',
    port=6379,
    password='',
    db=1)

# print(r.zcard('user_2'))
# list_user =['userId','movieId','rating']
# print(r.keys())
#
# for ee in r.keys():
#     count =r.zcard(ee)
#
#     if count>1000:
#         for index in r.zrange(ee,0,-1):
#             print(str(index))
#             items =index.decode("utf-8").split("_")
#             list_user.append([ee.decode("utf-8"),int(items[0]),float(items[1])])
#         print(ee)


# list_user = pd.DataFrame(list_user)
# print(list_user)
# user_data = list_user.pivot(index = 'userId', columns = 'movieId', values = 'rating').fillna(0)
# print(user_data)
# r.set('foo', 'bar')
# value = r.get('foo')
# print(value)
# def get_top_n(predictions, n=10):
#     # First map the predictions to each user.
#     top_n = defaultdict(list)
#     for uid, iid, true_r, est, _ in predictions:
#         top_n[uid].append((iid, est))
#
#     # Then sort the predictions for each user and retrieve the k highest ones.
#     for uid, user_ratings in top_n.items():
#         user_ratings.sort(key=lambda x: x[1], reverse=True)
#         top_n[uid] = user_ratings[:n]
#
#     return top_n
# #
# user_based start
ratings = pd.read_csv('ratings.csv')
# print(ratings)
X_train, X_test = train_test_split(ratings, test_size = 0.20, random_state = 34)
# ratings_train = pd.read_csv('ratings_train.csv')
user_data = X_train.pivot(index = 'userId', columns = 'movieId', values = 'rating').fillna(0)

# user_data = ratings_train['user_'+str(int(ratings.userId)) in list_user].pivot_table(index = 'userId', columns = 'movieId', values = 'rating').fillna(0)
dummy_train = X_train.copy()
dummy_test = X_test.copy()

dummy_train['rating'] = dummy_train['rating'].apply(lambda x: 0 if x > 0 else 1)
dummy_test['rating'] = dummy_test['rating'].apply(lambda x: 1 if x > 0 else 0)
dummy_train = dummy_train.pivot(index = 'userId', columns = 'movieId', values = 'rating').fillna(1)

# The movies not rated by user is marked as 0 for evaluation
dummy_test = dummy_test.pivot(index ='userId', columns = 'movieId', values = 'rating').fillna(0)

print("eee")
print(dummy_train.shape)
user_similarity = cosine_similarity(user_data)
user_similarity[np.isnan(user_similarity)] = 0
print(user_similarity)
print(user_similarity.shape)

user_predicted_ratings = np.dot(user_similarity, user_data)
print(user_predicted_ratings.shape)
user_final_ratings = np.multiply(user_predicted_ratings, np.array(dummy_train))

print(user_final_ratings[42])
pd.set_option("max_columns", None) #Showing only two columns

pd.set_option("max_rows", None)
# user_final_ratings = pd.DataFrame(user_final_ratings)
# print("ee")
# print(len(str(user_final_ratings.iloc[42].sort_values(ascending = False)[0:1000]).encode('utf-8')))# this is top 5
# #store it to redis
# for i in range(1484):
#     r.set("user_"+str(i),(str(user_final_ratings.iloc[i].sort_values(ascending = False)[0:1000])))

# user_based end



# here we get the

# reader = Reader(rating_scale=(1, 5))
# print("ee")
# X_train = pd.DataFrame(X_train)
# print("ee")
#
# data = Dataset.load_from_df(X_train[['userId', 'movieId', 'rating']], reader)# Use the famous SVD algorithm.
# trainset = data.build_full_trainset()
# algo = SVD()
# algo.fit(trainset)
# testset = trainset.build_anti_testset()
# predictions = algo.test(testset)
#
# top_n = get_top_n(predictions, n=10)

# Print the recommended items for each user
# for uid, user_ratings in top_n.items():
#     print(uid, [iid for (iid, _) in user_ratings])
# Run 5-fold cross-validation and print results.

#item item based
movie_features = X_train.pivot(index = 'movieId', columns = 'userId', values = 'rating').fillna(0)
item_similarity = cosine_similarity(movie_features)
item_similarity[np.isnan(item_similarity)] = 0
print(item_similarity)
print("- "*10)
print(item_similarity.shape)
item_predicted_ratings = np.dot(movie_features.T, item_similarity)
item_final_ratings = np.multiply(item_predicted_ratings, dummy_train)
item_final_ratings.head()
item_final_ratings.iloc[42].sort_values(ascending = False)[0:5]
for i in range(1484):
    r.set("item_"+str(i),(str(item_final_ratings.iloc[i].sort_values(ascending = False)[0:1000])))



# coding=utf-8
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# import pandas as pd
# import numpy as np
#
# df = pd.read_csv('ratings.csv')
# test_ratio = 0.2
# df['split'] = np.random.randn(df.shape[0], 1)
#
# msk = np.random.rand(len(df)) <= 0.8
#
# train = df[msk]
# print(train.size)
#
# test = df[~msk]
# print(test.size)
# print(test)
#
# train.to_csv('ratings_train.csv',index=False)
# test.to_csv('ratings_test.csv',index=False)
# np.random.seed(43)
# shuffled_indices = np.random.permutation(len(df))
# test_set_size = int(len(df) * test_ratio)
# test_indices = shuffled_indices[:test_set_size]
# train_indices = shuffled_indices[test_set_size:]
# train = df.iloc[train_indices]
# test = df.iloc[test_indices]
# print('finish shuffle!')
# train.to_csv('ratings_train.csv',index=False)
# test.to_csv('ratings_test.csv',index=False)
# print('finish saving!')


# data = pd.read_csv('ratings_train.csv')
#
