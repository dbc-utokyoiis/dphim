FROM ubuntu:22.04

RUN apt-get update

RUN apt-get install -y \
    build-essential \
    cmake \
    g++-11 \
    libboost-all-dev \
    libnuma1 libnuma-dev \
    libjemalloc-dev \
    wget

COPY . /workspace
RUN mkdir -p /workspace/dataset &&  cd /workspace/dataset && \
    wget http://www.philippe-fournier-viger.com/spmf/datasets/kosarak_utility_spmf.txt && \
    wget http://www.philippe-fournier-viger.com/spmf/datasets/chainstore.txt && \
    wget http://www.philippe-fournier-viger.com/spmf/datasets/BMS_utility_spmf.txt && \
    wget https://www.philippe-fournier-viger.com/spmf/datasets/accidents_utility_spmf.txt

RUN mkdir -p /workspace/build && cd /workspace/build && cmake .. && make run -j

WORKDIR /workspace
