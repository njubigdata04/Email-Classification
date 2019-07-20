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
    private static Map<String, String> classMap = new HashMap<String, String>();
/*
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
    }};*/

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
        int cnt = 0;
        String[] filterStrs = {"Xref", "Path", "From", "Message-ID", "Date", "Article-I.D.", "References", "Lines", "Expires", "Last-update"};
        while (scan.hasNextLine()) {
            String s = scan.nextLine();
            Boolean skip = false;
            for (String filter : filterStrs) {
                if (s.startsWith(filter)) {
                    skip = true;
                    break;
                }
            }
            if (skip) {
                continue;
            }
            stream = analyzer.tokenStream("renyi", new StringReader(s));
            CharTermAttribute cta = stream.addAttribute(CharTermAttribute.class);//保存响应词汇
            stream.reset();
            while (stream.incrementToken()) {
                String s1 = cta.toString();
                out.write((s1 + "\n").getBytes("utf-8"),0,(s1 + "\n").getBytes("utf-8").length);
                cnt++;
            }
            stream.end();
            stream.close();
        }
        scan.close();
        in.close();
        out.close();
        fsin.close();
        fsout.close();
        System.out.println(outputPath + "\t" + cnt);
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
        String outputPath = "/task3/purefiles";
        String classMapPath = "/task3/classMap.txt";
        if (args.length >= 4) {
            inputPath = args[0];
            stopWordsPath = args[1];
            outputPath = args[2];
            classMapPath = args[3];
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

        int cnt = 0;
        FileSystem fsout = FileSystem.newInstance(configuration);
        FSDataOutputStream out = fsout.create(new Path(classMapPath));
        for (Path p : listPath) {
            String[] nameSegments = p.toString().split("/");
            String className = nameSegments[nameSegments.length - 1];
            ++cnt;
            classMap.put(className, String.valueOf(cnt));
            String s = className + "\t" + cnt + "\n";
            out.write(s.getBytes("utf-8"),0, s.getBytes("utf-8").length);
        }
        out.close();
        fsout.close();

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