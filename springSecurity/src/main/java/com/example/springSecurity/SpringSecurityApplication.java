package com.example.springSecurity;

//import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;


public class SpringSecurityApplication {

	public static void main(String[] args) {
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

			Integer result=javaRDD.reduce((value1,value2) -> value1+value2);

			System.out.println("my result ******************************************************************************************************"+result);

			JavaRDD<Double> sqrtRdd = javaRDD.map(value -> Math.sqrt(value));

			sqrtRdd.foreach(value ->
						System.out.println(value)

			    );
			//count total no of elements in my rdd
			System.out.println(sqrtRdd.count());



			// Close Spark context
			sc.close();

		}

}
