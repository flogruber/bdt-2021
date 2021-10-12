package dev.grub3r;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.functions;

import java.util.Arrays;

public class TransformationAndAction {
    private SparkSession _spark;

    public TransformationAndAction()
    {

    }

    public void start(String source, String mode)
    {
        System.out.println("Hello World - TransformationAndAction");
        startSpark();

        Dataset<Row> dfInitial = readFileSourceCSV(source);
        dfInitial.printSchema();

        Dataset<Row> df = dfInitial;
        for(int i = 0; i<60; i++)
        {
            System.out.println("Run: " + i); 
            df = df.union(dfInitial);         
        }
        System.out.println("Dataset-Count: " + df.count());
        df.printSchema();

        df = cleanUpPerformanceExample(df);
        df.printSchema();

        stopSpark();
    }

    //region Spark
    private SparkSession setupSparkSession(){
        SparkSession spark = SparkSession.builder()
                                    .appName("Load Restaurants")
                                    .master("local")
                                    .getOrCreate();
         return spark;
    }

    public void startSpark()
    {
        System.out.println("*** SETTING UP SPARK SESSION ***");
        _spark = setupSparkSession();
    }

    public void stopSpark()
    {
        System.out.println("*** STOPPING SPARK SESSION ***");
        _spark.stop();
    }
    //endregion

    //region Read CSV
    private Dataset<Row> readFileSourceCSV(String source)
    {
        Dataset<Row> df = _spark.read().format("csv")
                    .option("header", "true")
                    .load(source);
        return df;
    }

    private Dataset<Row> cleanUpPerformanceExample(Dataset<Row> df)
    {
        df = df.withColumnRenamed("Lower Confidence Limit","lcl")
                .withColumnRenamed("Upper Confidence Limit","ucl");
        return df;
    }

    private Dataset<Row> transformPerformanceExample(Dataset<Row> df)
    {
        
        return df;
    }
    //endregion
}