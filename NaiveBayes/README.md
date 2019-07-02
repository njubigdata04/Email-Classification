NaiveBayes类是负责朴素贝叶斯的，通过三个参数，



```powershell
hadoop jar NaiveBayes <停词表> <类表> <训练样本> <测试集> <输出位置>
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

