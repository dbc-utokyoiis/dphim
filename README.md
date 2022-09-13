# DPHIM

Dynamic Parallelization for High-utility Itemset Mining (HUIM)

## Requirements

* Only Linux is supported

* A C++ compiler that supports C++20 or later.
    * GCC
    * Clang (>12.x) + libc++

* Boost library

* libnuma

* (Optional) libpmem2, libvmem
    * See https://github.com/pmem/pmdk

## Simple Source Code Description

* `include`, `src`
    * These directories have HUIM-specific source codes
    * `dphim::dphim_base` class (`include/dphim/dphim_base.hpp`) provides implementations shared among several HUIM algorithms
        * `dphim::dphim_base::parseTransactions()`: parsing of input files
        * `dphim::dphim_base::calcTWU()`: calculation of TWU (calcTWU)
    * `dphim::DPEFIM` class  (`include/dphim/dpefim.hpp`, `src/dpefim.cpp`) is a main class of DPHIM implementaion for EFIM algorithm
        * `dphim::DPEFIM::calcFirstSU()` mainly corresponds to Build step in the paper
        * `dphim::DPEFIM::search()` mainly corresponds to Search step in the paper
    * `dphim::DPFHM` class  (`include/dphim/dpfhm.hpp`) is a main class of DPHIM implementaion for FHM algorithm
        * `dphim::DPEFIM::calcFMAP()` mainly corresponds to Build step in the paper
        * `dphim::DPEFIM::search()` mainly corresponds to Search step in the paper
* `nova`
    * This directory contains a mechanism for task parallel execution
        * `nova/include/nova/task.hpp` has a implementation of a task management structure in C++ coroutine manner
        * `nova/include/nova/*_scheduler.hpp` provides implementations of various task scheduler
            * e.g., global task queue, local task queue, NUMA-aware local task queue

## Build

This project can be built using CMake.

```
$ mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make
```

## Execute

Please execute this command

```
$ ./run -a ${algorithm} -s ${execution method} -t ${# of thread} -i ${dataset} -o ${output} -m ${minutil}
```

* `${algorithm}` is an algorithm for HUIM
    * This implementation supports `efim` and `fhm`
* `${execution method}` should be one of `sp`, `global`, `local`, `local-numa` or `dphim`

* To run on persistent memory, you need to add `--pmem` option and execute with root privileges
    * for example
    ```
    $ sudo ./run -a efim -t ${# of threads} -i ${dataset} -o ${output} -m ${minutil} --pmem=numa
    ```

## Dataset

You can download datasets from [SPMF open-source repository](http://www.philippe-fournier-viger.com/spmf/index.php?link=datasets.php).

* Kosarak

```
$ wget http://www.philippe-fournier-viger.com/spmf/datasets/kosarak_utility_spmf.txt
```

* Chainstore

```
$ wget http://www.philippe-fournier-viger.com/spmf/datasets/chainstore.txt
```

* BMS

```
$ wget http://www.philippe-fournier-viger.com/spmf/datasets/BMS_utility_spmf.txt
```

* Accidents

```
$ wget https://www.philippe-fournier-viger.com/spmf/datasets/accidents_utility_spmf.txt
```

## Docker

You can use docker to execute DPHIM.

```
$ docker build . -t dphim
$ docker run -t dphim ./build/run -a ${algorithm} -i ${dataset} -o ${output} -m ${minutil}
```

This docker container downloads the above datasets in `dataset` directory in advance