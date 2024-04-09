package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class sqlData {
    public static void main(String args[]){

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true)
                .csv("C:\\Users\\91623\\Desktop\\springSecurity\\springSecurity\\src\\main\\resources\\exams\\students.csv");

        Long numberOfRows = dataset.count();
        System.out.println(numberOfRows);

        Row row = dataset.first();
        String subject = row.getAs("subject");
        System.out.println(subject);

        dataset.show();

        spark.close();
    }
}
