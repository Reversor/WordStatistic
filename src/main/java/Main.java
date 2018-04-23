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

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word count").setMaster("local[*]");
//        String[] garbage = new String[]{"\n", "", "и", "в"};
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaPairRDD<String, Long> ratingTolstoy =
                    wordRatingRDD(sc, "src/main/resources/Tolstoy/*.txt");
            JavaPairRDD<String, Long> ratingMarks =
                    wordRatingRDD(sc, "src/main/resources/Marks/*.txt");
            JavaPairRDD<String, Long> ratingDuma =
                    wordRatingRDD(sc, "src/main/resources/Duma/*.txt");
            List<Tuple2<Long, Iterable<String>>> result1 = ratingTolstoy.intersection(ratingMarks)
                    .map(Tuple2::swap).mapToPair(t -> t).groupByKey().sortByKey(false).takeAsync(20).get();
            System.out.println("Топ 20 слов из \"Война и мир\" Толстого в \"Капитал\" Маркса:");
            result1.forEach(System.out::println);
            List<Tuple2<Long, Iterable<String>>> result2 = ratingTolstoy
                    .subtractByKey(ratingMarks).mapToPair(Tuple2::swap).groupByKey()
                    .sortByKey(false).takeAsync(20).get();
            System.out.println("Топ 20 слов из \"Война и мир\" Толстого которых нет в \"Капитал\" Маркса:");
            result2.forEach(System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static JavaPairRDD<String, Long> wordRatingRDD(JavaSparkContext sc, String path) {
        return sc.textFile(path)
                .distinct()
                .flatMap(s -> Iterators.forArray(s.split("\\p{Space}")))
                .map(StringUtils::capitalize)
                .map(s -> s.replaceAll("\\p{Blank}|\\p{Graph}", ""))
                .subtract(sc.textFile("src/main/resources/garbage.txt"))
                .filter(s -> s.length() > 1)
                .mapToPair(s -> new Tuple2<>(s, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(t -> t)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }
}
