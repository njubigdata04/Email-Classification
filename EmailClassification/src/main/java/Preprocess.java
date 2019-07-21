import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Preprocess {
    private Map<String, Integer> WordNum = new HashMap<String, Integer>();
    private Map<String, ArrayList<String>> MapResult = new HashMap<String, ArrayList<String>>();
    public Preprocess(String wordPath) throws FileNotFoundException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(wordPath));
        Scanner scanner = new Scanner(in);
        while (scanner.hasNextLine()){
            String line = scanner.nextLine();
            String[] parts = line.split("\t");
            WordNum.put(parts[0], Integer.parseInt(parts[1]));
        }
        scanner.close();
        in.close();
    }
    public void transform(String inputPath, String outputPath)throws IOException{
        Configuration conf = new Configuration();
        removeOutputFolder(conf, outputPath);
        FileSystem fsin = FileSystem.newInstance(conf);
        FileSystem fsout = FileSystem.newInstance(conf);
        FSDataOutputStream out = fsout.create(new Path(outputPath));
        FSDataInputStream in = fsin.open(new Path(inputPath));
        Scanner scanner = new Scanner(in);
        while(scanner.hasNextLine()){
            String line=  scanner.nextLine();
            String[]parts = line.split("\t");
            if(parts.length < 2){
                System.out.println("Wrong reading input file!" + line);
                System.exit(-1);
            }
            String filenameclassword = parts[0];
            String tfidf = parts[1];
            String[] sp = filenameclassword.split(":");
            String word = sp[1];
            if(!WordNum.containsKey(word)){
                System.out.println("No matched word in input file!" + word + "in" + filenameclassword);
                //System.exit(-1);
                continue;
            }
            Integer wordno = WordNum.get(word);
            String fileclass = sp[0];
            ArrayList<String> re = new ArrayList<String>();
            if(MapResult.containsKey(fileclass)){
                re = MapResult.get(fileclass);
            }
            re.add(wordno.toString() + ":" + tfidf);
            MapResult.put(fileclass, re);
        }
        System.out.println("output size" + MapResult.size());
        for(Map.Entry<String, ArrayList<String>>entry : MapResult.entrySet()){
            String fn = entry.getKey();
            //System.out.println(fn);
            String tmp_fn = fn.split("-")[1];
            //out.writeChars(fn.split("-")[1]);
            out.write(tmp_fn.getBytes("utf-8"),0,  tmp_fn.getBytes().length);
            ArrayList<String> re = entry.getValue();
            for(String part:re){
                //out.writeChars(" "+ part);
                String s = " " + part;
                out.write(s.getBytes("utf-8"),0,  s.getBytes().length);

            }
            String nextline = "\n";
            out.write(nextline.getBytes("utf-8"),0,  nextline.getBytes().length);
            //out.writeChars("\n");
        }
        scanner.close();
        in.close();
        out.close();
        fsin.close();
        fsout.close();

    }
    private static void removeOutputFolder(Configuration configuration, String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            System.out.println("File has exist!and now deleting...");
            fileSystem.delete(path, true);
        }
    }
    public static void main(String[] args) throws Exception{
        String sourcePath = "/task3/TFIDF-out/part-r-00000";
        String wordPath = "/task3/feature-out/part-r-00000";
        String outputPath = "/task3/jige";
        if (args.length >= 3) {
            wordPath = args[0];
            sourcePath = args[1];
            outputPath = args[2];
        } else {
            System.out.println("args error");
            System.exit(-1);
            return;
        }
        System.out.println("init...");
        Preprocess pre = new Preprocess(wordPath);
        System.out.println("transforming...");
        pre.transform(sourcePath, outputPath);
        System.out.println("finish!");
    }
}
