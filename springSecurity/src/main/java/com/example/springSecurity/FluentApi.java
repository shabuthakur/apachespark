package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class FluentApi {

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

        // Create RDD apply fluent api
         sc.parallelize(inputData)
                 .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1l))
                 .reduceByKey((value1,value2)-> value1+value2)
                 .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances "));

        // Create Pair Rdd
//        JavaPairRDD<String, Long> pairRDD = javaRDD.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1l));


        sc.close();
    }

}
