import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;

import java.io.IOException;

import org.apache.lucene.analysis.util.CharArraySet;


public class SkipWords {

    private static Map<String, String> classMap = new HashMap<String, String>() {{
        put("comp.graphics", "1");
        put("comp.os.ms-windows.misc", "2");
        put("comp.sys.ibm.pc.hardware", "3");
        put("comp.sys.mac.hardware", "4");
        put("comp.windows.x", "5");
        put("rec.autos", "6");
        put("rec.sport.baseball", "7");
        put("rec.sport.hockey", "8");
        put("sci.crypt", "9");
        put("sci.electronics", "10");
        put("sci.space", "11");
        put("soc.religion.christian", "12");
        put("talk.politics.guns", "13");
        put("talk.politics.mideast", "14");
        put("talk.politics.misc", "15");
        put("talk.religion.misc", "16");
    }};

    /*
        public static class SkipWordsMapper extends Mapper<Object, Text, Text, Text> {

            private final Text one = new Text("1");
            private Text label = new Text();
            private String[] sws;
            private Analyzer analyzer;
            Map<String, String> classMap = new HashMap<String, String>() {{
                put("comp.graphics", "1");
                put("comp.os.ms-windows.misc", "2");
                put("comp.sys.ibm.pc.hardware", "3");
                put("comp.sys.mac.hardware", "4");
                put("comp.windows.x", "5");
                put("rec.autos", "6");
                put("rec.sport.baseball", "7");
                put("rec.sport.hockey", "8");
                put("sci.crypt", "9");
                put("sci.electronics", "10");
                put("sci.space", "11");
                put("soc.religion.christian", "12");
                put("talk.politics.guns", "13");
                put("talk.politics.mideast", "14");
                put("talk.politics.misc", "15");
                put("talk.religion.misc", "16");
            }};

            @Override
            public void setup(Context context) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                Path[] path = DistributedCache.getLocalCacheFiles(conf);
                FileSystem fs = FileSystem.getLocal(conf);
                FSDataInputStream in = fs.open(path[0]);
                Scanner scan = new Scanner(in);
                ArrayList<String> arrayList = new ArrayList<String>();
                while (scan.hasNext()) {
                    String s = scan.next();
                    arrayList.add(s);
                }
                scan.close();
                in.close();
                sws = new String[arrayList.size()];
                for (int i = 0; i < arrayList.size(); i++) {
                    sws[i] = arrayList.get(i);
                }
                analyzer = new MyStopAnalyzer(sws);
            }

            @Override
            protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                    throws IOException, InterruptedException {
                InputSplit inputSplit = context.getInputSplit();

                String fileName = getInputSplitFileName(inputSplit);
                String className = getInputSplitClass(inputSplit);

                TokenStream stream = null;
                stream = analyzer.tokenStream("renyi", new StringReader(value.toString()));
                CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
                //在lucene 4 以上  要加入reset 和  end方法
                stream.reset();

                Configuration conf = context.getConfiguration();
                String uri = tmpFilePath + "/" + fileName + "-" + className;

                FileSystem fs1 = FileSystem.newInstance(URI.create(uri), conf);
                FSDataOutputStream out = null;
                Path path = new Path(uri);
                if (fs1.exists(path)) {
                    out = fs1.append(new Path(uri));
                } else {
                    out = fs1.create(new Path(uri));
                }

                while (stream.incrementToken()) {
                    String s = cta.toString();
                    label.set(s + ":" + fileName);
                    context.write(label, one);
                    out.writeChars(s + " ");
                }
                stream.end();
                stream.close();
                out.close();
                fs1.close();
            }

            private String getInputSplitFileName(InputSplit inputSplit) {
                String fileFullName = ((FileSplit) inputSplit).getPath().toString();
                String[] nameSegments = fileFullName.split("/");
                return nameSegments[nameSegments.length - 1];
            }

            private String getInputSplitClass(InputSplit inputSplit) {
                String fileFullName = ((FileSplit) inputSplit).getPath().toString();
                String[] nameSegments = fileFullName.split("/");
                String name = nameSegments[nameSegments.length - 2];
                return classMap.get(name);
            }

        }

        public static class SkipWordsReducer extends Reducer<Text, Text, Text, Text> {
            private int idx = 0;

            @Override
            protected void reduce(Text key, Iterable<Text> values,
                                  Reducer<Text, Text, Text, Text>.Context context)
                    throws IOException, InterruptedException {
                if (values == null) {
                    return;
                }
                idx++;
                context.write(key, new Text(String.valueOf(idx)));
            }
        }
    */
    public static class MyStopAnalyzer extends Analyzer {
        private Set stops;

        public MyStopAnalyzer(String[] sws) {
            // 将字符串数组添加到停用词的set集合中
            stops = StopFilter.makeStopSet(sws, true);
            // 加入原来的停用词
            stops.addAll(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            //正则匹配分词
            Tokenizer source = new LowerCaseTokenizer();
            CharArraySet charArraySet = CharArraySet.copy(stops);
            return new TokenStreamComponents(source, new StopFilter(source, charArraySet));
        }

    }

    private static void writePureFile(String inputPath, String outputPath, Analyzer analyzer) throws FileNotFoundException, IOException {
        String[] nameSegments = inputPath.split("/");
        String fileName = nameSegments[nameSegments.length - 1];
        String fileClass = classMap.get(nameSegments[nameSegments.length - 2]);
        outputPath = outputPath + "/" + fileName + "-" + fileClass;
        Configuration conf = new Configuration();
        FileSystem fsin = FileSystem.newInstance(conf);
        FileSystem fsout = FileSystem.newInstance(conf);
        FSDataOutputStream out = fsout.create(new Path(outputPath));
        FSDataInputStream in = fsin.open(new Path(inputPath));
        Scanner scan = new Scanner(in);
        TokenStream stream = null;
        while (scan.hasNext()) {
            String s = scan.next();
            stream = analyzer.tokenStream("renyi", new StringReader(s));
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
            stream.reset();
            while (stream.incrementToken()) {
                String s1 = cta.toString();
                out.writeChars(s1 + '\n');
            }
            stream.end();
            stream.close();
        }
        scan.close();
        in.close();
        out.close();
        fsin.close();
        fsout.close();
    }

    private static Analyzer readStopWords(String stopWordsPath) throws FileNotFoundException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(stopWordsPath));
        Scanner scan = new Scanner(in);
        ArrayList<String> arrayList = new ArrayList<String>();
        while (scan.hasNext()) {
            String s = scan.next();
            System.out.println(s+"\t -------------------");
            arrayList.add(s);
        }
        scan.close();
        in.close();
        String[] sws = new String[arrayList.size()];
        for (int i = 0; i < arrayList.size(); i++) {
            sws[i] = arrayList.get(i);
        }
        return new MyStopAnalyzer(sws);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/task3/emails";
        String stopWordsPath = "/task3/Stop_words.txt";
        String outputPath = "/task3/purefile-out";
        if (args.length >= 3) {
            inputPath = args[0];
            stopWordsPath = args[1];
            outputPath = args[2];
        } else {
            System.out.println("args error");
            System.exit(-1);
            return;
        }
        Analyzer analyzer = readStopWords(stopWordsPath);

        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.newInstance(URI.create(inputPath), configuration);
        removeOutputFolder(configuration, outputPath);
        FileStatus[] fs = hdfs.listStatus(new Path(inputPath));
        Path[] listPath = FileUtil.stat2Paths(fs);
        System.out.println("Hello ---------------------------------");
        System.out.println(listPath.length);
        for (Path p : listPath) {
            FileStatus[] fs1 = hdfs.listStatus(p);
            Path[] listPath1 = FileUtil.stat2Paths(fs1);
            for (Path p1 : listPath1) {
                System.out.println(p1.toString()+"--------------------");
                writePureFile(p1.toString(), outputPath, analyzer);
            }
        }

    }

    private static void removeOutputFolder(Configuration configuration, String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    public static void display(String str, Analyzer a) {
        TokenStream stream = null;
        try {
            stream = a.tokenStream("renyi", new StringReader(str));
            //  PositionIncrementAttribute pia = stream.addAttribute(PositionIncrementAttribute.class);  //保存位置
            // OffsetAttribute oa = stream.addAttribute(OffsetAttribute.class); //保存辞与词之间偏移量
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
            // TypeAttribute ta = stream.addAttribute(TypeAttribute.class); //保存类型
            //在lucene 4 以上  要加入reset 和  end方法
            stream.reset();
            while (stream.incrementToken()) {
                //     System.out.println(pia.getPositionIncrement() + ":[" + cta.toString() + "]:" + oa.startOffset() + "->" + oa.endOffset() + ":" + ta.type());
                System.out.println(cta.toString());
            }
            stream.end();
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}