NaiveBayes类是负责朴素贝叶斯的，通过三个参数，
 hadoop jar ~/cjh/BigData-1.0-SNAPSHOT.jar Bayes /task3/Bjyclass/part-r-00000 /task3/purefiles /task3/purefiles /task3/BjyBayes



```powershell
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