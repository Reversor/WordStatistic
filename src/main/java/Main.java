import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

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
            Tuple2<Long, Tuple2<Long, Long>> combine;
            wordsCountTolstoy
                    .union(wordsCountDuma)
                    .combineByKey(
                            //инициализируем (кол-во встреч, счетчик)
                            v -> Tuple2.apply(v, 1),
                            //суммируем кол-во встреч, считаем сколько раз проссумировали, вызывается когда повторно нашли значение
                            (t, v) -> Tuple2.apply(t._1() + v, t._2() + 1),
                            //собираем показатели
                            (c1, c2) -> Tuple2.apply(c1._1 + c2._1, c1._2 + c2._2)
                    )
                    .filter(t -> t._2._2 > 1)
                    .mapValues(t -> t._2 / t._1) //frequency?
                    .take(5).forEach(System.out::println);

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
