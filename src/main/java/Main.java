import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/* Hadoop configuration class: get and set
http://hadoop.apache.org/docs/r2.6.4/api/org/apache/hadoop/conf/Configuration.html?is-external=true */
/* Hadoop job class: main class standard frame work for MapReduce
https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Job.html */

//args[0]input file to build Library,  args[1]out file to save Library, args[2]nGram
//args[3]threshold,  args[4]top k selected

public class Main {
        public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

            //Library Builder
            Configuration conf1 = new Configuration(); //initialize
            //Hadoop: configuration.set(String name, String value) --> Set the "value" of the "name" property.
            conf1.set("textinputformat.record.delimiter", ".");
            conf1.set("nGram", args[2]);

            //Hadoop: job set up, connect classes
            Job job1 = Job.getInstance();
            job1.setJobName("nGram");
            job1.setJarByClass(Main.class);

            job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
            job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            TextInputFormat.setInputPaths(job1, new Path(args[0]));
            TextOutputFormat.setOutputPath(job1, new Path(args[1]));
            // wait for job1 to be done, then start job 2; job 2 needs job 1 result as input
            job1.waitForCompletion(true);

            //Library Optimizer
            Configuration conf2 = new Configuration();
            conf2.set("threashold", args[3]);
            conf2.set("top", args[4]);

            DBConfiguration.configureDB(conf2,               //DB connect to database
                    "com.mysql.jdbc.Driver",
                    "jdbc:mysql://ip_address:port/test",
                    "root",
                    "password");

            Job job2 = Job.getInstance(conf2);
            job2.setJobName("Model");
            job2.setJarByClass(Main.class);

            job2.addArchiveToClassPath(new Path("path_to_ur_connector"));

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Output_Writer.class);
            job2.setOutputValueClass(NullWritable.class);

            job2.setMapperClass(NGramLibraryOptimizer.Map.class);
            job2.setReducerClass(NGramLibraryOptimizer.Reduce.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(DBOutputFormat.class);

            DBOutputFormat.setOutput(job2, "output",
                    new String[] {"starting_phrase", "following_word", "count"});

            TextInputFormat.setInputPaths(job2, args[1]);
            job2.waitForCompletion(true);
        }

    }

