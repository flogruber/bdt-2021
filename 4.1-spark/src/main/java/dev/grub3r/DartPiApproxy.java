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
import org.apache.spark.sql.Encoders;
import java.util.ArrayList;
import java.util.List;

public class DartPiApproxy {
    private SparkSession _spark;
    
    public class PerformanceModel {
        public long Start;
        public long Get_Session;
        public long Initial_DF;
        public long Build_Larger_Datasheet;
        public long Clean_UP;
        public long Perform_Tranformations;
        public long Take_Actions;
        public long Done;

        public PerformanceModel()
        {
            Start = 0;
            Get_Session = 0;
            Initial_DF = 0;
            Build_Larger_Datasheet = 0;
            Clean_UP = 0;
            Perform_Tranformations = 0;
            Take_Actions = 0;
            Done = 0;
        }
    }

    public DartPiApproxy()
    {

    }

    public void start(int slices)
    {
        PerformanceModel pfModel = new PerformanceModel();
        int numberOfThrows = 100000 * slices;
        System.out.println("Throws: " + numberOfThrows);

        pfModel.Start = System.currentTimeMillis();
        _spark = SparkSession.builder().appName("Spark Pi").master("local").getOrCreate();
        _spark.sparkContext().setLogLevel("OFF");
        pfModel.Get_Session = System.currentTimeMillis();

        List<Integer> l = new ArrayList<>(numberOfThrows);
        for(int i = 0; i < numberOfThrows; i++)
        {
            l.add(i);
        }

        Dataset<Row> incDF = _spark.createDataset(l, Encoders.INT()).toDF();
        pfModel.Initial_DF = System.currentTimeMillis();

        ClacTimes(pfModel);
        _spark.stop();
    }

    private void ClacTimes(PerformanceModel model)
    {
        long t1 = model.Get_Session - model.Start;
        long t2 = model.Initial_DF - model.Get_Session;

        System.out.println("*** Performance ***");
        System.out.println("Session: " +t1);
        System.out.println("Initial dataframe: " +t2);
    }
}