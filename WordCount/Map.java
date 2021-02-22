import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     * MapReduce Map方法
     * 输入键值对：<行号，字符串>
     * 输出键值对：<单词，1>
     * @param o
     * @param text
     * @param outputCollector
     * @param reporter
     * @throws IOException
     */
    @Override
    public void map(Object o, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String line = text.toString();    // 获取输入行
        StringTokenizer tokenizer = new StringTokenizer(line);     // 按空格切分字符串
        // 遍历切分结果，每一个token就是一个单词
        while(tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            outputCollector.collect(word, one);
        }
    }
}