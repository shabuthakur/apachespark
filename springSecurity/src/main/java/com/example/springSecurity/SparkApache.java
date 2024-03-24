package com.example.springSecurity;

//import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;


public class SparkApache {
    public static void main(String[] args){
        List<Integer> inputData = new ArrayList<>();
        inputData.add(22);
        inputData.add(26);
        inputData.add(333);
        inputData.add(89);

        // Set logger level
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Set Spark configuration
        SparkConf sparkConf = new SparkConf()
                .setAppName("Starting spark")  // Set your application name here
                .setMaster("local[*]");      // Use local mode for testing

        // Create Spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Create RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(inputData);

        JavaRDD< Tuple2<Integer,Double>> sqrtRdd = javaRDD.map(value ->  new Tuple2<>(value,Math.sqrt(value)));

        sqrtRdd.foreach(value -> System.out.println(value));


        // if you want 4 values in tuple then you use tuple 5
        Tuple5<Integer, Double,Integer,Integer,Integer> myValue = new Tuple5<>(2,3.4,3,4,5);
        System.out.println(myValue);



        sc.close();

    }




    }
