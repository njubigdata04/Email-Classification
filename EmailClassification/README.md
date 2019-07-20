<center> <font color=#000000 size=6 face="黑体">邮件自动分类Hadoop MapReduce程序运行指南</font></center>
## 特征选择 

首先对原始训练数据进行停词和分词，输入为20_newsgroup原始训练数据文件夹，输出为分词后的干净文本，以及类名和类编号的对应关系。运行命令为：

```shell
hadoop jar EmailClassification.jar SkipWords /data/task3/20_newsgroup /data/task3/Skip_words.txt /user/2019st04/task3/trainData/purefiles /user/2019st04/task3/classmap.txt
```

然后对干净文本进行特征提取，输出为文本特征全集。运行命令为：

```shell
hadoop jar EmailClassification.jar FeatureExtraction /user/2019st04/task3/trainData/purefiles /user/2019st04/task3/feature-out
```

## 特征向量权重计算

利用上个任务中生成的干净文本，计算各个单词在各个文件中的TF-IDF值，输入为干净文本文件夹，输出为TF-IDF文件。运行命令为：

```shell
hadoop jar EmailClassification.jar FeatureExtraction /user/2019st04/task3/trainData/purefiles /user/2019st04/task3/trainData/TFIDF-out
```



NaiveBayes类是负责朴素贝叶斯的，通过三个参数，
 hadoop jar ~/cjh/BigData-1.0-SNAPSHOT.jar Bayes /task3/Bjyclass/part-r-00000 /task3/purefiles /task3/purefiles /task3/BjyBayes



```shell
hadoop jar NaiveBayes <停词表> <类表> <训练样本> <输出位置>
```



```
hadoop jar Bayes <类表> <训练样本> <输出位置>
```



GetClassNum是负责输出类以及类中文件数量的

```powershell
hadoop jar GetClassNum <停词表> <类表> <训练样本> <测试集> <输出位置>
```

```
URI stopPath = new URI(args[0]);
URI classPath = new URI(args[1]);
String inputTrainPath = args[2];
String inputTestPath = args[3];
String outputPath = args[4];
```

公式
$$
P(class|X) = \frac{P(X|class)P(class)}{P(X)}
$$

$$
P(X|class) = 连乘\frac{P(x在class中出现的次数)}{P(class类中的单词数 + 词库中的单词数)}
$$

在训练过程中产生文件tmp保存的是《classname#单词， 次数》

GetClassNum得到的是每个类的文件数

每个类的单词数

Bayes读入的是类编号\t文件数，训练样本是一行一个单词，文件名为“文件名-类编号”



Train的命令是

```
hadoop jar Train <类与文件数表> <训练样本> <输出位置>
```

Predict命令是

```
hadoop jar LogPredict <类与文件对应表> <训练样本输出>/part-r-00000 <预测样本> <输出位置>
```

hadoop jar Predict /tmp/2019st04/classfile /tmp/2019st04/BayesTrain/part-r-00000 /user/2019st04/task3/TestData/purefiles /tmp/2019st04/BayesPredict