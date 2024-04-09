package com.example.springSecurity;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

public class InMemoryDataStructures {
    public static void main(String args[]) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingsql").master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .getOrCreate();

        List<Row> inMemory = new ArrayList<Row>();

        inMemory.add(RowFactory.create("WARN","2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL","2015-12-31 03:19:32"));
        inMemory.add(RowFactory.create("WARN","2016-12-31 04:18:32"));
        inMemory.add(RowFactory.create("FATAL","2016-12-31 05:19:32"));
        inMemory.add(RowFactory.create("WARN","2013-1-31 04:19:90"));
        inMemory.add(RowFactory.create("WARN","2016-5-31 67:19:32"));
        inMemory.add(RowFactory.create("INFO","2016-18-31 77:19:32"));

        StructField[] structFields = new StructField[]{
                new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                new StructField("datetime",DataTypes.StringType,false,Metadata.empty())
        };

        StructType schema = new StructType(structFields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory,schema);


        //GROUP BY DATA

       dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> result = spark.sql("select level,collect_list(datetime) from logging_table group by level order by level");

        result.show();




        spark.close();
    }


    }
