import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/*https://hadoop.apache.org/docs/r3.0.0/api/org/apache/hadoop/mapreduce/lib/db/DBWritable.html*/
/* implement DBWritable to write to database*/

    public class Output_Writer implements DBWritable {
        //declare variables
        private String starting_phrase;
        private String following_word;
        private int count;

        //constructor
        public Output_Writer(String starting_prhase, String following_word, int count) {
            this.starting_phrase = starting_prhase; //"i love big"
            this.following_word = following_word; //"data"
            this.count= count; //10
        }

        public void readFields(ResultSet arg0) throws SQLException {
            this.starting_phrase = arg0.getString(1);
            this.following_word = arg0.getString(2);
            this.count = arg0.getInt(3);

        }

        // write to database = arg0 into three columns: "i love big" "data" 10
        public void write(PreparedStatement arg0) throws SQLException {
            arg0.setString(1, starting_phrase);
            arg0.setString(2, following_word);
            arg0.setInt(3, count);

        }

    }


