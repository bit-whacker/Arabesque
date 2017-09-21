title: Download Arabesque
project: Arabesque
---


# Github repo

The source code can be downloaded from Github
* [Giraph](https://github.com/qcri/Arabesque/tree/master)
* [Spark](https://github.com/qcri/Arabesque/tree/spark-2.0)

Arabesque is licensed under Apache 2.0.

# Precompiled Java package

The precompiled jar contains the complete Arabesque system along with all its dependencies. It also includes 4 example applications:
clique finding, motif counting, frequent subgraph mining and triangle counting.

This jar is ideal if you simply want to test the example applications or manually setup your own project.

* [Download the latest Giraph JAR](https://qbox.qcri.org/s/ZpA823NgPQqDzB8)
* [Download the latest Spark JAR](https://qbox.qcri.org/s/xvlvdDPGiEFYNjK)

To run any of the example applications, refer to [our user guide](user_guide.html#how-to-run-an-arabesque-job).

# Preconfigured Maven project

If you want to start developing your own graph mining implementations on top of Arabesque, the easiest way to achieve this is by forking our [Arabesque-Skeleton](https://github.com/qcri/Arabesque-Skeleton) project and follow the instructions on the README file.

# Arabesque tools

* [Virtualbox image](https://qbox.qcri.org/s/Yfnxi9J3Bkkf1lH)
* Docker image `docker run -it dsqcri/arabesque` (select user `su arabesque`)
* [Installation scripts](https://qbox.qcri.org/s/HiSHoQ2YSPFrjet) to install Arabesque stack on a single node