import com.google.common.collect.Iterators;
import com.sun.org.apache.xalan.internal.xsltc.compiler.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word count").setMaster("local[*]");
//        String[] garbage = new String[]{"\n", "", "и", "в"};
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            String pathTolstoy = "src/main/resources/Tolstoy/*.txt";
            String pathMarks = "src/main/resources/Marks/*.txt";
            String pathDuma = "src/main/resources/Duma/*.txt";
            wordRatingRDD(sc, pathTolstoy);
            wordRatingRDD(sc, pathMarks);
//            top20(sc, ).forEach(System.out::println);
//            sequenceFile.take(20).forEach(System.out::println);
        }
    }

    static JavaPairRDD<Long, Iterable<String>> wordRatingRDD(JavaSparkContext sc, String path) {
        JavaRDD<String> garbage = sc.textFile("src/main/resources/garbage.txt");

        return sc.textFile(path)
                .distinct()
                .flatMap(s -> Iterators.forArray(s.split("\\p{Space}")))
                .map(String::toLowerCase)
                .map(s -> s.replaceAll("\\p{Punct}", ""))
                .filter(s -> s.length() > 1)
                .subtract(garbage)
                .mapToPair(s -> new Tuple2<>(s, 1L))
                .reduceByKey(Long::sum)
                .map(Tuple2::swap)
                .mapToPair(t -> t)
                .groupByKey()
                .sortByKey(false);
    }
}
