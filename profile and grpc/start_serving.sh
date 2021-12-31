#!/bin/sh


# Start TensorFlow Serving container and open the REST API port
# -p 端口映射
# -v 卷映射，本地地址:docker目标地址
# -e 环境变量MODEL_NAME和卷映射目标地址保持一致

docker run -p 8501:8501 -p 8500:8500 -v "D:\Other\tf-serving-example\savedmodel:/models/simple_cnn_model" -e MODEL_NAME=table_data -t tensorflow/serving --rm

docker run -p 8501:8501 -p 8500:8500 -v /D/Codes/testTFserving/TFServingTools:/models/ner -e MODEL_NAME=ner -t tensorflow/serving

docker run -p 8501:8501 --name table_data --mount type=bind,source=/D/Codes/separateData/saved_model/table_data,target=/models/table_data/0 -e MODEL_NAME=table_data/0 -t tensorflow/serving