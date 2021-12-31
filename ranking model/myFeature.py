import numpy as np
import pandas as pd
from typing import Tuple, List, Any
import tensorflow as tf

from tensorflow import feature_column
from tensorflow.keras import layers
from tensorflow.keras.models import load_model
from sklearn.model_selection import train_test_split

dense_features = ["user_average", "user_count", 'user_frequency', "movie_release", "movie_average", "movie_count"]


def define_model(
        feature_columns: List[Any],
        optimizer: str = "adam",
        loss: str = "binary_crossentropy",
        metrics: List[str] = ["accuracy", 'AUC'],
) -> tf.keras.Model:
    feature_layer = tf.keras.layers.DenseFeatures(feature_columns)
    model = tf.keras.Sequential(
        [
            feature_layer,
            layers.Dense(128, activation="relu"),
            layers.Dense(64, activation="relu"),
            layers.Dense(1, activation="sigmoid"),
        ]
    )

    model.compile(optimizer=optimizer, loss=loss, metrics=metrics)
    return model


def save_model(model: tf.keras.Model, version_number: int = 0):
    save_model_path = f"./saved_model/rank_model/{version_number}"
    model.save(save_model_path, save_format='tf')
    print(f"saved model to {save_model_path}")


def df_to_dataset(
        dataframe: pd.DataFrame, shuffle: bool = True, batch_size: int = 32
) -> tf.data.Dataset:
    dataframe = dataframe.copy()
    labels = dataframe.pop("label")
    ds = tf.data.Dataset.from_tensor_slices((dict(dataframe), labels))
    # if shuffle:
    #     ds = ds.shuffle(buffer_size=len(dataframe))
    ds = ds.batch(batch_size)
    return ds


def str2seq(input_str: str):
    seq = input_str.split('|')
    return seq


def main():
    train_filepath = "final_input.csv"
    test_filepath = "output_list.csv"
    train_data = pd.read_csv(train_filepath)
    test_data = pd.read_csv(test_filepath)

    # 删除不必要的id
    final_data = test_data[['uid', 'mid','label']]
    train_data = train_data.drop(['uid', 'mid'], axis=1)
    test_data = test_data.drop(['uid', 'mid'], axis=1)

    # 填充空白数据
    train_data[dense_features] = train_data[dense_features].fillna(0)
    test_data[dense_features] = test_data[dense_features].fillna(0)
    # 将频率为inf的数据修改为1
    train_data.replace([np.inf, -np.inf], 1.0, inplace=True)
    test_data.replace([np.inf, -np.inf], 1.0, inplace=True)


    train_ds = df_to_dataset(train_data)
    test_ds = df_to_dataset(test_data, shuffle=False)
    feature_columns = []

    for header in ["user_average", "user_count", 'user_frequency','user_highrate', "movie_release"
        , "movie_average",'movie_highrate', "movie_count"]:
        feature_columns.append(feature_column.numeric_column(header))

    u_avg = feature_column.numeric_column("user_average")
    u_avg_buckets = feature_column.bucketized_column(
        u_avg, boundaries=[0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5]
    )
    feature_columns.append(u_avg_buckets)

    u_cnt = feature_column.numeric_column("user_count")
    u_cnt_buckets = feature_column.bucketized_column(
        u_cnt, boundaries=[10, 20, 50, 100, 200, 500, 1000, 5000, 10000]
    )
    feature_columns.append(u_cnt_buckets)

    u_freq = feature_column.numeric_column("user_frequency")
    u_freq_buckets = feature_column.bucketized_column(
        u_freq, boundaries=[1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 0.1, 1]
    )
    feature_columns.append(u_freq_buckets)

    u_hrate = feature_column.numeric_column("user_highrate")
    u_hrate_buckets = feature_column.bucketized_column(
        u_hrate, boundaries=[0.1, 0.3, 0.5, 0.7, 0.9]
    )
    feature_columns.append(u_hrate_buckets)

    m_rel = feature_column.numeric_column("movie_release")
    m_rel_buckets = feature_column.bucketized_column(
        m_rel, boundaries=[1960, 1970, 1980, 1990, 2000, 2010]
    )
    feature_columns.append(m_rel_buckets)

    m_avg = feature_column.numeric_column("movie_average")
    m_avg_buckets = feature_column.bucketized_column(
        m_avg, boundaries=[0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5]
    )
    feature_columns.append(m_avg_buckets)

    m_cnt = feature_column.numeric_column("movie_count")
    m_cnt_buckets = feature_column.bucketized_column(
        m_cnt, boundaries=[10, 100, 500, 1000, 2000, 5000, 10000, 50000, 100000]
    )
    feature_columns.append(m_cnt_buckets)

    m_hrate = feature_column.numeric_column("movie_highrate")
    m_hrate_buckets = feature_column.bucketized_column(
        m_hrate, boundaries=[0.1, 0.3, 0.5, 0.7, 0.9]
    )
    feature_columns.append(m_hrate_buckets)

    u_favor = feature_column.categorical_column_with_vocabulary_list(
        "user_favor", ['Adventure', 'Animation', 'Children', 'Comedy', 'Fantasy', 'Romance', 'Drama', 'Action',
                       'Crime', 'Thriller', 'Horror', 'Mystery', 'Sci-Fi', 'IMAX', 'Documentary', 'War',
                       'Musical', 'Western', 'Film-Noir', '(no genres listed)']
    )
    u_favor_one_hot = feature_column.indicator_column(u_favor)
    feature_columns.append(u_favor_one_hot)

    m_genre = feature_column.categorical_column_with_vocabulary_list(
        "movie_genre", ['Adventure', 'Animation', 'Children', 'Comedy', 'Fantasy', 'Romance', 'Drama', 'Action',
                        'Crime', 'Thriller', 'Horror', 'Mystery', 'Sci-Fi', 'IMAX', 'Documentary', 'War',
                        'Musical', 'Western', 'Film-Noir', '(no genres listed)']
    )
    m_genre_one_hot = feature_column.indicator_column(m_genre)
    feature_columns.append(m_genre_one_hot)

    model = define_model(feature_columns=feature_columns)
    # model = load_model('./saved_model/rank_model/0')
    model.fit(train_ds, epochs=2)
    test_loss, test_accuracy, test_auc = model.evaluate(test_ds)

    print('\n\nTest Loss {}, Test Accuracy {}, Test AUC {}'.format(test_loss, test_accuracy, test_auc))

    save_model(model, 0)
    predictions = model.predict(test_ds)
    print(predictions)
    final_pri = pd.DataFrame(predictions,columns=['predict'])
    final_data = pd.concat([final_data,final_pri],axis=1)

    final_data.to_csv('t1.csv',index=False)
    # 显示部分结果
    for prediction, survived in zip(predictions, list(test_ds)[0][1]):
        print("Predicted survival: {:.2%}".format(prediction[0]),
              " | Actual outcome: ",
              ("Ture" if bool(survived) else "False"))


if __name__ == "__main__":
    main()
