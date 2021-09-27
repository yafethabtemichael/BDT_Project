package cs523.FinalProject;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProjSparkSQLProcess {
	public static void main(String[] args) throws AnalysisException {

		//Logger.getLogger("org").setLevel(Level.OFF);
		
		final SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[*]");
        sparkConf.set("hive.metastore.uris", "thrift://localhost:9083");

        
        final SparkSession sparkSession = SparkSession.builder().appName("Spark SQL-Hive").config(sparkConf)
                .enableHiveSupport().getOrCreate();
        
        Dataset<Row> tabledata = sparkSession.sql("select * from TwitterData");                     //quering it 
        tabledata.show();
        
	}

}
