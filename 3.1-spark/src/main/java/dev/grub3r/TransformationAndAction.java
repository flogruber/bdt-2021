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
    
    public class PerformanceModel {
        public long Get_Session;
        public long Load_DataSheet;
        public long Build_Larger_Datasheet;
        public long Clean_UP;
        public long Perform_Tranformations;
        public long Take_Actions;
        public long Done;

        public PerformanceModel()
        {
            Get_Session = 0;
            Load_DataSheet = 0;
            Build_Larger_Datasheet = 0;
            Clean_UP = 0;
            Perform_Tranformations = 0;
            Take_Actions = 0;
            Done = 0;
        }
    }

    public TransformationAndAction()
    {

    }

    public void start(String source, String mode)
    {
        PerformanceModel pfModel = new PerformanceModel();
        System.out.println("Hello World - TransformationAndAction");
        pfModel.Get_Session = System.currentTimeMillis();
        startSpark();

        pfModel.Load_DataSheet = System.currentTimeMillis();
        Dataset<Row> dfInitial = readFileSourceCSV(source);
        dfInitial.printSchema();

        pfModel.Build_Larger_Datasheet = System.currentTimeMillis();
        Dataset<Row> df = dfInitial;
        for(int i = 0; i<60; i++)
        {
            System.out.println("Run: " + i); 
            df = df.union(dfInitial);         
        }
        System.out.println("Dataset-Count: " + df.count());
        df.printSchema();

        pfModel.Clean_UP = System.currentTimeMillis();
        df = cleanUpPerformanceExample(df);
        df.printSchema();

        pfModel.Perform_Tranformations = System.currentTimeMillis();
        df = transformPerformanceExample(df, mode);

        pfModel.Take_Actions = System.currentTimeMillis();
        df.collect();
        pfModel.Done = System.currentTimeMillis();

        ClacTimes(pfModel, df);
        stopSpark();
    }

    //region Spark
    private SparkSession setupSparkSession(){
        SparkSession spark = SparkSession.builder()
                                    .appName("Load Restaurants")
                                    .master("local")
                                    .getOrCreate();
         
        spark.sparkContext().setLogLevel("OFF");
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

    private Dataset<Row> transformPerformanceExample(Dataset<Row> df, String mode)
    {
        if(mode.compareToIgnoreCase("transform")!=0)
        {
            df = df.withColumn("avg",functions.expr("(lcl+ucl)/2"))
                    .withColumn("lcl2",df.col("lcl"))
                    .withColumn("ucl2",df.col("ucl"));
            if(mode.compareToIgnoreCase("drop")==0)
            {
                df = df.drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        }
        return df;
    }
    //endregion

    public void ClacTimes(PerformanceModel model, Dataset<Row> df)
    {
        long p1 = model.Load_DataSheet - model.Get_Session;
        long p2 = model.Build_Larger_Datasheet - model.Load_DataSheet;
        long p3 = model.Clean_UP - model.Build_Larger_Datasheet;
        long p4 = model.Perform_Tranformations - model.Clean_UP;
        long p5 = model.Take_Actions - model.Perform_Tranformations;
        long p6 = model.Done - model.Take_Actions;
        long p7 = model.Done - model.Get_Session;
        
        System.out.println("*** Performance ***");
        System.out.println("Session: " + p1);
        System.out.println("Load initial dataset: " + p2);
        System.out.println("Building full dataset: " + p3);
        System.out.println("Clean-up: " + p4);
        System.out.println("Transformations: " + p5);
        System.out.println("Final actions: " + p6);
        System.out.println("Total: " + p7);
        System.out.println("# of records: " + df.count());
    }
}