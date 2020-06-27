import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/* Build a library containing all 2,3,n-1-Gram pairs. Final Output: "n-->1" User enter n-1 Gram phrase, get the next 1 word counts*/
/* The Hadoop Map-Reduce framework: https://hadoop.apache.org/docs/r2.7.4/api/org/apache/hadoop/mapreduce/Mapper.html */

public class NGramLibraryBuilder {
    /* class to extend Mapper */
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int nGram; // number of words in each phrase

        //Step 1: the Hadoop Map-Reduce framework calls "set up" at the beginning of the task.
        // configuration interchange parameters
        @Override
        public void setup(Context context) {
            //getInt(String name, int defaultValue --> Get the value of the name property as an int.
            // default value = if configuration arg is null, pass default value
            Configuration conf = context.getConfiguration();
            nGram = conf.getInt("nGram", 5);
        }

        //Step 2: the Hadoop Map-Reduce framework calls "map"
        // Text: a string; convenient to serialize and move the data around
        // IntWritable: an int; convenient to serialize and move the data around; support read and write function

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Edge case
            if((value == null) || (value.toString().trim()).length() == 0) {
                return;
            }

            // "LongWritable key" is the offset of a sentence in an article
            //"Text value" is the line that was read
            String line = value.toString().trim().toLowerCase();
            line = line.replaceAll("[^a-z]", " ");

            //An array holding all the words in a line
            String[] words = line.split("\\s+"); //split by ' ', '\t'...ect

            //If array has less one word, go to the next line
            if (words.length < 2) {
                return;
            }

            //Java StringBuilder
            //Output: if nGram =5, output will be "i love", "i love big", "i love big data"，
            //"i love big data class" but cannot do "i love big data class today" as 6 > 5Gram
            StringBuilder sb;
            for (int i = 0; i < words.length - 1; i++) {
                sb = new StringBuilder();
                sb.append(words[i]); //append current word Word[i]

                //for each word[i], loop to get the phrases <= nGram
                for (int j = 1; i + j < words.length && j < nGram; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    //write phrases as Text, count =1 in IntWriteable, write to HDFS temporarily
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }


    /* class to extend Reducer*/
     public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //Step 3: the Hadoop Map-Reduce framework calls "reduce"

        @Override
            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                int sum = 0;
                for(IntWritable value: values) {
                    sum += value.get(); //count number of times the phrase Text in the Mapper
                }
                context.write(key, new IntWritable(sum)); //write the Text and the number of times to context
            }
        }
    }




