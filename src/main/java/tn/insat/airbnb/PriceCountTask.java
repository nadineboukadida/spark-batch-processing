package tn.insat.airbnb;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;
public class PriceCountTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(PriceCountTask.class);

    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new PriceCountTask().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputDir) {
        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(PriceCountTask.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> csvFile = sc.textFile(inputFilePath);

        String threshold = "137";
        System.setProperty("hadoop.home.dir", "C:/Users/nadine/Desktop/bigdata/winutils/hadoop-3.0.0");
        JavaPairRDD<String, Integer> countPairRDD = sc.parallelizePairs(
                Collections.singletonList(new Tuple2<>("count", (int) csvFile.filter(row -> {
                    String[] columns = row.split(",");
                    String price =columns[1];
                    return price.compareTo(threshold)> 0;
                }).count())));
        countPairRDD.saveAsTextFile(outputDir);
    }
}
