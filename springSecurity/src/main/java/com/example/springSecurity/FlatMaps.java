package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMaps   {
    public static void main(String[] args){
        List<String> inputData = new ArrayList<>();
        inputData.add( "WARN: Tuesday 4 September 0405");
        inputData.add("WARN: Tuesday 5 September 0408");
        inputData.add("ERROR: Wednesday 5 September 0409");
        inputData.add("WARN: Tuesday 6 September 0430");
        inputData.add("FATAL: Tuesday 7 September 0429");

        // Set logger level
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Set Spark configuration
        SparkConf sparkConf = new SparkConf()
                .setAppName("Starting spark")  // Set your application name here
                .setMaster("local[*]");      // Use local mode for testing

        // Create Spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRDD= sc.parallelize(inputData);

       JavaRDD<String> words = javaRDD.flatMap(value -> Arrays.asList(value.split(" ")).iterator());

       //filter means that you dont want data
       JavaRDD<String> filter = words.filter(word -> word.length()>1);

//       words.foreach(value -> System.out.println(value));

       filter.foreach(word -> System.out.println(word));


        sc.close();
    }

}
