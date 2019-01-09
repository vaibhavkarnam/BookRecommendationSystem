Team - Data_Mayhem
Fall 2018

Trained an Ensemble prediction model to suggest the most relevant books based on historical big data. This involved bootstrap sampling of the large data set, training and prediction accomplished using Amazon Elastic MapReduce.

Code author
-----------
Vaibhav Karnam

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 2.9.1
- Maven
- AWS CLI (for EMR execution)

Environment
-----------

The preprocessed input is available at https://docs.google.com/uc?id=15hUsDMnaeVsNE2U56K5Vd_QNS0pu4C3w&export=download

Please use the preprocessed data as input for the KNNImpl program which is the K nearest neighbor classifier program
In the program arguments place set the folder path of your training data for local.params and was.training. Also for 
Running on local please set add k to the arguments or set aws.k 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.input} ${local.output} ${local.params} 3

For running ensemble bootstrapping please change job name to TrainingEnsemble and provide input data which fits into memory

If you want to create more models please use the bootstrap sampling program with KNN training data as the input

Bootstrapped input has already been run and is available at https://drive.google.com/file/d/1L2bd0xjZDUSzC94rGalQ-s_9zXf_VC7g/view?usp=sharing. Please use this as aws does not allow
Strong such big data into memory

For running the ensemble training and classification
Please provide the saved models in training as the local.params or aws.training
Provide the bootstrap sampled input as input folder and change job name to EnsembleClassifier


1) Example ~/.bash_aliases:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

Execution
---------
All of the build & execution commands are organized in the Makefile.

Please run PreProcessRawTwitterData.java with the twitterdataset.
The preprocessed data is then given as the input for Preprocessing mapreduce job to give the 
final processed data in csv format which can then be used by the weka models

PreProcess_ARFF_CSV.java and PreProcess_ConvertCSV_ARFF.java are for converting the preprocessed csv format
data to ARFF for use by weka training and classification 
 

1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
