import org.junit.Test;

import static org.junit.Assert.*;

public class WordCountTest {

    @Test
    public void debug() throws Exception {

        String inputPath  = "file:///Users/Vivek/Desktop/WPI/4th Year/C-Term/CS 4445 - Big Data/Projects/CS433_Project0/data.txt";
        String outputPath = "file:///Users/Vivek/Desktop/WPI/4th Year/C-Term/CS 4445 - Big Data/Projects/CS433_Project0/output/";

        String[] args = new String[2];
        args[0] = inputPath;
        args[1] = outputPath;

        WordCount wc = new WordCount();
        wc.debug(args);
    }

}