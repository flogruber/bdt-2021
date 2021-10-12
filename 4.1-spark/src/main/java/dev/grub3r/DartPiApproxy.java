package dev.grub3r;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DartPiApproxy implements Serializable {
    private static final long serialVersionUID = -1546L;
    private static long counter = 0;
    private SparkSession _spark;
    
    private final class PerformanceModel {
        public long Start;
        public long Get_Session;
        public long Initial_DF;
        public long Throwing_Darts_Done;
        public long Analyzing;
        public long Perform_Tranformations;
        public long Take_Actions;
        public long Done;

        public PerformanceModel()
        {
            Start = 0;
            Get_Session = 0;
            Initial_DF = 0;
            Throwing_Darts_Done = 0;
            Analyzing = 0;
            Perform_Tranformations = 0;
            Take_Actions = 0;
            Done = 0;
        }
    }

    private final class DartMapper implements MapFunction<Row, Integer> {
        private static final long serialVersionUID = 38446L;

        @Override
        public Integer call(Row r) throws Exception {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            counter ++;
            if(counter % 100000 == 0){
                System.out.println("" + counter + " darts thrown so far");
            }
            return (x * x + y * y <= 1) ? 1 : 0;
        }
    }

    private final class DartReducer implements ReduceFunction<Integer> {
        private static final long serialVersionUID = 12859L;

        @Override
        public Integer call(Integer x, Integer y) {
            return x + y;
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

        List<Integer> listOfThrows = new ArrayList<>(numberOfThrows);
        for(int i = 0; i < numberOfThrows; i++)
        {
            listOfThrows.add(i);
        }

        Dataset<Row> incDF = _spark.createDataset(listOfThrows, Encoders.INT()).toDF();
        pfModel.Initial_DF = System.currentTimeMillis();

        Dataset<Integer> dartsDs = incDF.map(new DartMapper(), Encoders.INT());
        pfModel.Throwing_Darts_Done = System.currentTimeMillis();

        int dartsInCircle = dartsDs.reduce(new DartReducer());
        pfModel.Analyzing = System.currentTimeMillis();

        ClacTimes(pfModel);
        System.out.println("Pi is roughly " + 4.0 *dartsInCircle / numberOfThrows);

        _spark.stop();
    }

    private void ClacTimes(PerformanceModel model)
    {
        long t1 = model.Get_Session - model.Start;
        long t2 = model.Initial_DF - model.Get_Session;
        long t3 = model.Throwing_Darts_Done - model.Initial_DF;
        long t4 = model.Analyzing - model.Throwing_Darts_Done;

        System.out.println("*** Performance ***");
        System.out.println("Session: " +t1);
        System.out.println("Initial dataframe: " +t2);
        System.out.println("Throwing darts done in " + t3);
        System.out.println("Analyzing results in: " + t4);
    }
}