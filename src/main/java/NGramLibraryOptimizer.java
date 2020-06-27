import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/* Count and compare frequency of 1 word after a n-1 Gram phrase; */
/* Set up a threshold*/
/* Select only the top frequency*/

public class NGramLibraryOptimizer {
        public static class Map extends Mapper<LongWritable, Text, Text, Text> {

            int threashold;
            // get the threashold parameter from the configuration
            @Override
            public void setup(Context context) {
                //configuration reads threshold
                Configuration conf = context.getConfiguration();
                threashold = conf.getInt("threashold", 20);
            }

            // input "i love big data\t10"
            // output: "i love big": "data" =10

            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                //Edge case
                if((value == null) || (value.toString().trim()).length() == 0) {
                    return;
                }
                //Read line into an Array[], Array[0] = "i love big data"; Array[1] = 10
                String line = value.toString().trim();
                String[] wordsPlusCount = line.split("\t");

                //Filter: read next line
                if(wordsPlusCount.length < 2) {
                    return;
                }

                //Read phrase into an Array[] of words
                String[] phrase = wordsPlusCount[0].split("\\s+");  //split with space(s)
                int count = Integer.valueOf(wordsPlusCount[1]);

                //Filter: if Array[1] = 10 < threshold, read next line
                if(count < threashold) {
                    return;
                }

                //Combine phrase into a string "i love big"
                StringBuilder sb = new StringBuilder();
                for(int i = 0; i < phrase.length-1; i++) {
                    sb.append(phrase[i]).append(" ");
                }

                String outputKey = sb.toString().trim();
                String outputValue = phrase [phrase.length - 1]; //get "data"

                //write "i love big", write "data=10" to HDFS temporarily
                if(!((outputKey == null) || (outputKey.length() <1))) {
                    context.write(new Text(outputKey), new Text(outputValue + "=" + count));
                }
            }
        }



        public static class Reduce extends Reducer<Text, Text, Output_Writer, NullWritable> { //writer to Output_Writer

            int top; // get the top k parameter from the configuration
                     // only put the top k results in the database; better to use PriorityQueue
            @Override
            public void setup(Context context) {
                Configuration conf = context.getConfiguration();
                top = conf.getInt("top", 5);
            }

            @Override
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                // key = "i love big"
                // values <IntWritable> = <"data=10", "apple=9",...>
                TreeMap<Integer, List<String>> treemap = new TreeMap<Integer, List<String>>(Collections.reverseOrder()); //sort in descending order

                for(Text val: values) { //example: pull "data=10"
                    String curValue = val.toString().trim(); // curValue = "data=10"
                    String word = curValue.split("=")[0].trim(); //word ="data"
                    int count = Integer.parseInt(curValue.split("=")[1].trim()); //count = 10

                    //USE "COUNT" AS THE KEY FOR FILTERING
                    // if 10 in this treemap, add "data"
                    if(treemap.containsKey(count)) {
                        treemap.get(count).add(word);
                    }

                    //if not contained in treemap, put (count, list of words under the count)
                    else {
                        List<String> list = new ArrayList<String>();
                        list.add(word);
                        treemap.put(count, list);
                    }
                }

                //Iterate treemap, still within reducer
                //treemap is a sorted data structure, it looks like < <10,<data>>, <50, <girl, bird>> <60, <cat, dog>>
                Iterator<Integer> iter = treemap.keySet().iterator();
                //select top words
                for(int j=0; iter.hasNext() && j<top;) { // top k defines how many needs to be written in the database
                    int keyCount = iter.next();    //10,50...
                    List<String> words = treemap.get(keyCount); //get the listing <data>, <girl, bird>

                    for(String curWord: words) {
                        // write to OutputWriter database： key="i love big", currentword = "data", keyCount = 10
                        context.write(new Output_Writer(key.toString(), curWord, keyCount),NullWritable.get());
                        j++; //so when the for loop is done, pointer goes to next integer"50"
                    }
                }
            }
        }
    }


