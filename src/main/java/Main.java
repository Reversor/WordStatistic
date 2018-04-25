import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Word count").setMaster("local[*]");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaPairRDD<String, Long> wordsCountTolstoy = wordCountsRDD(sc, "Tolstoy/*.txt");
//            JavaPairRDD<String, Long> wordsCountMarks = wordCountsRDD(sc, "Marks/*.txt");
            JavaPairRDD<String, Long> wordsCountDuma = wordCountsRDD(sc, "Duma/*.txt");

            String tolstoyDuma = "result/TolstoyDuma";
            cleanDir(tolstoyDuma);
            wordsCountTolstoy.subtractByKey(wordsCountDuma)
                    .mapToPair(Tuple2::swap)
                    .groupByKey().sortByKey(false)
                    .coalesce(1).saveAsTextFile(tolstoyDuma);

            Long countTolstoy = wordsCountTolstoy.count();
            Long countDuma = wordsCountDuma.count();
            String wordsFrequency = "result/wordsFreaquency";
            cleanDir(wordsFrequency);
            wordsCountDuma.join(wordsCountTolstoy)
                    .filter(t -> (double) t._2._1 / countDuma > (double) t._2._2 / countTolstoy)
                    .mapToPair(t -> new Tuple2<>(t._2._1, t._1))
                    .union(wordsCountDuma.subtractByKey(wordsCountTolstoy).mapToPair(Tuple2::swap))
                    .groupByKey().sortByKey(false)
                    .coalesce(1).saveAsTextFile(wordsFrequency);

        }
    }

    private static void cleanDir(String strPath) throws IOException {
        Path path = Paths.get(strPath);
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static JavaPairRDD<String, Long> wordCountsRDD(JavaSparkContext sc, String path) {
        String nonAlphabet = "[^A-Za-zА-Яа-я]";
        String space = Pattern.compile("\\p{Space}|\\h", Pattern.UNICODE_CHARACTER_CLASS).pattern();

        return sc.textFile(path)
                .map(s -> s.replace("\\d+", ""))
                .distinct()
                .flatMap(s -> Iterators.forArray(s.split(space)))
                .filter(s -> s.length() > 1)
                .map(s -> s.replaceAll(nonAlphabet, ""))
                .map(String::toLowerCase)
                .subtract(sc.textFile("garbage.txt"))
                .mapToPair(s -> new Tuple2<>(s, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(t -> t)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }
}
