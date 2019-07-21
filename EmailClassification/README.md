<center> <font color=#000000 size=6 face="黑体">邮件自动分类Hadoop MapReduce程序运行指南</font></center>
## 特征选择 

首先对原始训练数据进行停词和分词，输入为20_newsgroup原始训练数据文件夹，输出为分词后的干净文本，以及类名和类编号的对应关系。运行命令为：

```shell
hadoop jar EmailClassification.jar SkipWords /data/task3/20_newsgroup /data/task3/Stop_words.txt /user/2019st04/task3new/trainData/purefiles /user/2019st04/task3new/classmap.txt
```

然后对干净文本进行特征提取，输出为文本特征全集。运行命令为：

```shell
hadoop jar EmailClassification.jar FeatureExtraction /user/2019st04/task3new/trainData/purefiles /user/2019st04/task3new/feature-out
```

## 特征向量权重计算

利用上个任务中生成的干净文本，计算各个单词在各个文件中的TF-IDF值，输入为干净文本文件夹，输出为TF-IDF文件。运行命令为：

```shell
hadoop jar EmailClassification.jar Client /user/2019st04/task3new/trainData/purefiles /user/2019st04/task3new/trainData/TFIDF-out
```

## 格式转换

将上一步输出的TF-IDF转换成需要的格式。需要用到上一部分的输出以及单词与单词编号的对应关系。运行命令为：

```
hadoop jar EmailClassification.jar Preprocess /user/2019st04/task3new/feature-out/part-r-00000  /user/2019st04/task3new/trainData/TFIDF-out/part-r-00000 /user/2019st04/task3new/formatted_TFIDF.txt
```

同时对于测试数据文件，也要进行上述三个步骤的计算，运行命令与训练数据基本相同，此处不再赘述。此处我们假设输出在testData文件夹下。

## 获得类编号对应的文件数

利用之前任务生成的干净文本，计算每个类编号对应的文件数

```shell
hadoop jar EmailClassification.jar GetClassNum <分词后的干净文本> <输出位置>
```

具体举例问

```shell
 hadoop jar ~/EmailClassification.jar GetClassNum /user/2019st04/task3new/trainData/purefiles /user/2019st04/task3new/trainData/FileCal
```



## 朴素贝叶斯

#### 训练   

需要用到上一步得到的类与相应文件数量，Train的命令是

```shell
hadoop jar EmailClassification.jar Train <类与文件数表> <训练样本> <输出位置>
```

```shell
hadoop jar EmailClassification.jar Train /tmp/2019st04/GetClassNum /user/2019st04/task3/purefiles /tmp/2019st04/BayesTrain
```

#### 预测

需要用到训练的结果输出以及类与对应文件数量，输入为分词后干净的测试样本集。

Predict命令是

```
hadoop jar EmailClassification.jar LogPredict <类与文件对应表> <训练样本输出>/part-r-00000 <预测样本> <输出位置>
```

```shell
hadoop jar EmailClassification.jar LogPredict /tmp/2019st04/GetClassNum /tmp/2019st04/BayesTrain/part-r-00000 /user/2019st04/task3/TestData/purefiles /tmp/2019st04/FinalPredictBayes3
```

