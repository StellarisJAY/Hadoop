import java.io.*;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws IOException {
        File file1 = new File("D://test.txt");
        File file2 = new File("D://result.txt");

        FileInputStream inputStream1 = new FileInputStream(file1);
        FileInputStream inputStream2 = new FileInputStream(file2);
        BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream1, "UTF-8"));
        BufferedReader reader2 = new BufferedReader(new InputStreamReader(inputStream2, "UTF-8"));

        String line1 = "";
        String line2 = "";
        int same = 0;

        while((line1 = reader1.readLine()) != null && (line2 = reader2.readLine()) != null){
            String word1 = line1.split("\t")[0];
            String word2 = line2.split("\t")[1];
            if(word1.equals(word2)){
                same++;
            }
        }
        System.out.println((double)same / 2000);

        reader1.close();
        reader2.close();
        inputStream1.close();
        inputStream2.close();
    }
}
