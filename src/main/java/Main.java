import com.google.common.collect.Iterators;
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
            String wordsFreaquency = "result/wordsFreaquency";
            cleanDir(wordsFreaquency);
            wordsCountTolstoy.join(wordsCountDuma)
                    .filter(t -> (double) t._2._1 / countTolstoy > (double) t._2._2 / countDuma)
                    .mapToPair(t -> new Tuple2<>(t._2._1, t._1))
                    .groupByKey().sortByKey(false)
                    .coalesce(1).saveAsTextFile(wordsFreaquency);

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
        String nonAlphabet = Pattern.compile("[^A-Za-zА-Яа-я]", Pattern.UNICODE_CHARACTER_CLASS).pattern();
        String space = Pattern.compile("\\p{Space}", Pattern.UNICODE_CHARACTER_CLASS).pattern();
        return sc.textFile(path)
                .distinct()
                .flatMap(s -> Iterators.forArray(s.split(space)))
                .map(String::toLowerCase)
                .map(s -> s.replaceAll(nonAlphabet, ""))
                .subtract(sc.textFile("garbage.txt"))
                .filter(s -> s.length() > 1)
                .mapToPair(s -> new Tuple2<>(s, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(t -> t)
                .persist(StorageLevel.MEMORY_AND_DISK());
    }
}
