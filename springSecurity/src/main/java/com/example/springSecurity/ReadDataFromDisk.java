package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadDataFromDisk {
    public static void main(String[] args){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("Starting spark")  // Set your application name here
                .setMaster("local[*]");      // Use local mode for testing

        // Create Spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sc.textFile("C:\\Users\\91623\\Desktop\\springSecurity\\springSecurity\\src\\main\\resources\\static\\input.txt");

        inputFile.foreach(words -> System.out.println(words));

        sc.close();

    }
}
