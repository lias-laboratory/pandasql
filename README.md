# PandaSQL

This project concerns implementing and optimizing Randomized Triangle Enumeration Algorithm using SQL queries.

## Requirements

* Cluster: 8 hosts
* Operating System: Ubuntu 16.04 LTE or higher
* Language: Python 2.6 or higher
* Libraries: Python_vertica client
* DBMS: Vertica, please refer to the official "documentation":https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/InstallationGuide/Other/InstallationGuide.htm

## Downloads

The source code of the project can be loaded here: https://github.com/lias-laboratory/pandasql/releases

It contains the following elements:

* CLI version (_cli_ directory) which contains:
  * Vertica_codes: contains two python scripts:
    * _Standard_TE.py_ concerns the triangle enumeration using standard algorithm query,
    * _Randomized_TE_ concerns the triangle enumeration using randomized algorithm optimized queries.
  * _Triplet_: this folder contains color triplet files according to the size of the cluster (8,27,64,…)
* GUI version (_gui_ directory) with HTML page interface

## Build and install

To use the script, please define your database connection statement in the script then use the following command line to execute it:

* For standard algorithm query: 

```
$ python Vertica_codes/Standard_TE.py path_to_your_dataset path_to_output_directory type[directed/undirected]
```

* For Randomized algorithm query:

```
$ python Vertica_codes/Randomized_TE.py path_to_your_dataset triplet/triplet8.txt path_to_output_directory type[directed/undirected]
```

In the command line above, make sure to choose between directed or undirected without typing key word type (this should be according to the type of the chosen data set). Here an example:

```
$ python Vertica_codes/Standard_TE.py Datasets/Real/WikiTalk.txt Results_TE/ directed
```

To use the graphic interface, update the file config.py  in PandaSQL directory with your database connection statement then use the following command line to execture it:

```
$ cd path/to/PandaSQL_GUI
$ python server.py
```

To use PandaSQL, open a browser and type: 127.0.0.1:5000

Please refer to this video for a demonstration on how to use it

[![Watch the video](https://img.youtube.com/vi/pwcYkOUV8_s/default.jpg)](https://youtu.be/pwcYkOUV8_s)

## Results

The results output using the CLI version of PandaSQL by the standard algorithm are of the following format: (vertex1,vertex2,vertex3)

Example of output:

|_. Vertex1 |_. vertex2 |_. vertex3 |
|     1     |     2     |     5     |
|     1     |     2     |     8     |
|     1     |     3     |     7     |
|    ...    |    ...    |    ...    |
|    185    |    200    |    305    |

The results output using the CLI version of PandaSQL by the randomized algorithm are of the following format: (machine,vertex1,vertex2,vertex3)

Example of output:

|_. machine |_. vertex1 |_. vertex2 |_. vertex3 |
|     1     |     1     |     2     |     3     |
|     1     |     1     |     2     |     5     |
|     1     |     1     |     3     |     7     |
|    ...    |    ...    |    ...    |    ...    |
|     8     |    160    |    59     |    365    |

## Publication

* Abir Farouzi, Ladjel Bellatreche, Carlos Ordonez, Gopal Pandurangan, Mimoun Malki. A Scalable Randomized Algorithm for Triangle Enumeration on Graphs based on SQL Queries, DAWAK Conference 2020

## Historic Contributors

* [Abir FAROUZI](https://www.lias-lab.fr/members/abirfarouzi/)
* [Ladjel BELLATRECHE](https://www.lias-lab.fr/members/ladjelbellatreche/)
* Carlos ORDONEZ
* Gopal PANDURANGAN
* Mimoun MALKI


