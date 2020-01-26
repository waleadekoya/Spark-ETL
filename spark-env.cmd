
set PYSPARK_PYTHON=C:\Users\Sesan\Anaconda3\python.exe
set HADOOP_HOME=C:\Spark\spark-2.4.4-bin-hadoop2.7
set JAVA_HOME=C:/Program Files/Java/jdk1.8.0_241

set PATH=$JAVA_HOME/bin:$PATH
set PATH=$HADOOP_HOME/lib/native
set HADOOP_CONF_DIR=%HADOOP_HOME%\conf

set JAVA_LIBRARY_PATH=$HADOOP_HOME/lib/native:$JAVA_LIBRARY_PATH
set HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
set HADOOP_OPTS="-Djava.library.path=$HADOOP_COMMON_LIB_NATIVE_DIR"
