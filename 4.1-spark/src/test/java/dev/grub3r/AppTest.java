package dev.grub3r;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {
    // Adopt to your local setup: /Users/martin/Software/FHSTP/repos
    // String pathCSVFile = "../data/input/spark/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv";
    // String pathJSONFile = "../data/input/spark/Restaurants_in_Durham_County_NC.json";

    DartPiApproxy cut;

    @Before
    public void tearUp() {
        cut = new DartPiApproxy();
    }

    @After
    public void tearDown(){
        // cut.stopSpark();
    }

    @Test
    public void shouldReturnDataSet() {
        cut.start(10);
    }
}
