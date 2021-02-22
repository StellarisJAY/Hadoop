package reducer;

import mapper.TrainMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;

public class NBReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    static Map<String, Integer> model;
    static long index = 1;
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();

        int countGood = model.get("好评");
        int countBad = model.get("差评");
        System.out.println(countGood + "," +countBad);
        int total = countBad + countGood;

        double pGood = (double)countGood / total;
        double pBad = (double)countBad / total;
        String tempLine = "";
        while(iterator.hasNext()){
            String line = iterator.next().toString();
            tempLine = line;
            String[] words = line.split(" ");
            for(String word : words){
                if(model.get("好评_"+word) != null){
                    double pWordGood = (double)model.get("好评_"+word);
                    pWordGood /= countGood;
                    pGood *= pWordGood;
                }
                else if(TrainMapper.isChinese(word)){
                    pGood *= 0;
                }
                if(model.get("差评_"+word) != null){
                    double pWordBad = (double)model.get("差评_"+word);
                    pWordBad /= countBad;
                    pBad *= pWordBad;
                }
                else if(TrainMapper.isChinese(word)){
                    pBad *= 0;
                }
            }
            double temp = pBad + pGood;
            pBad /= temp;
            pGood /= temp;


        }

        context.write(new LongWritable(index), new Text(pGood > pBad ? "好评" : "差评"));
        index++;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        model = readModel2(File.separator + "opt" + File.separator + "model1.txt");
    }

    public static void main(String[] args) throws IOException {
        Map<String, Integer> map = readModel2("D://model1.txt");
        System.out.println("好评".hashCode());
        Set<String> keySet = map.keySet();
        for(String k : keySet){
            System.out.println(k + "," + k.hashCode());
        }
        System.out.println(map.get("好评"));
    }

    private static Map<String, Integer> readModel(String path) throws IOException {
        Configuration conf = new Configuration();

        //指定使用的是HDFS文件系统
        conf.set("fs.defaultFS","hdfs://192.168.154.128:9000");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        String line = "";

        Map<String, Integer> map = new HashMap<>();
        while((line = reader.readLine()) != null){
            String[] contents = line.split("\t");
            map.put(new String(contents[0].getBytes(), 0, contents[0].length(), "UTF-8"), Integer.valueOf(contents[1]));
        }

        reader.close();
        inputStream.close();
        return map;
    }

    private static Map<String, Integer> readModel2(String path) throws IOException {
        File file = new File(path);
        FileInputStream inputStream = new FileInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));

        String line = "";
        Map<String, Integer> map = new HashMap<>();
        while((line = reader.readLine()) != null){
            String[] contents = line.split("\t");
            map.put(contents[0], Integer.valueOf(contents[1]));
        }

        reader.close();
        inputStream.close();
        return map;
    }
}
