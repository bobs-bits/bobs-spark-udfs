package org.bobsbits.bobs.sparksql.udfs;
//https://www.programcreek.com/java-api-examples/?code=jgperrin/net.jgp.labs.spark/net.jgp.labs.spark-master/src/main/java/net/jgp/labs/spark/l150_udf/BasicUdfFromTextFile.java

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import java.io.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;


import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;


public class BasicUDF implements Serializable {
    private static final long serialVersionUID = 3492970200940899011L;

    public static void main(String[] args) {
        System.out.println("Working directory = " + System.getProperty("user.dir"));
        BasicUDF app = new BasicUDF();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder().appName("CSV to Dataset").master("local").getOrCreate();

        //registers a new internal UDF
        /*
        spark.udf().register("x2Multiplier", new UDF1<String, String>() {
            private static final long serialVersionUID = -5372447039252716846L;

            @Override
            public Integer call(Integer x) {
                return x * 2;
            }
        }, DataTypes.IntegerType);
*/


        //xslt
        spark.udf().register("xslt", new UDF2<String, String, String>() {
            private static final long serialVersionUID = -7372447039252716846L;

            @Override
            public String call(String x, String y) {

                //TODO - ho to handle nll inputs?
                if (x == null) return null;
                //TODO - how to most naturally handle exceptions?
                //TODO - how to write to a log?
                //TODO - xslt is passed in as path; ths should be path to resource in udf jar; how to get this resource?
                //TODO - am I creating too many strings?
                try {
                    ByteArrayOutputStream baos = null;
                    StreamSource stylesource = new StreamSource(y);

                    InputStream xmlStream = new ByteArrayInputStream(x.getBytes());
                    StreamSource source = new StreamSource(xmlStream);

                    TransformerFactory factory = TransformerFactory.newInstance();
                    Transformer transformer = factory.newTransformer(stylesource);

                    baos = new ByteArrayOutputStream();
                    StreamResult result = new StreamResult(baos);
                    transformer.transform(source, result);
                    return baos.toString();
                } catch (Exception e){
                    e.printStackTrace();
                    return null;
                }
            }
        }, StringType);


        //xslt test

        String xmlS0 = "<?xml version=\"1.0\"?>\n" +
                "<?xml-stylesheet type=\"text/xsl\" href=\"xslt_test.xsl\"?>\n";
                String xmlS =
                "<Article>\n" +
                "  <Title>My Article</Title>\n" +
                "  <Authors>\n" +
                "    <Author>Mr. Foo</Author>\n" +
                "    <Author>Mr. Bar</Author>\n" +
                "  </Authors>\n" +
                "  <Body>This is my article text.</Body>\n" +
                "</Article>";

        String xsltS = "<?xml version=\"1.0\"?>\n" +
                "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">\n" +
                "\n" +
                "  <xsl:output method=\"text\"/>\n" +
                "\n" +
                "  <xsl:template match=\"/\">\n" +
                "    Article - <xsl:value-of select=\"/Article/Title\"/>\n" +
                "    Authors: <xsl:apply-templates select=\"/Article/Authors/Author\"/>\n" +
                "  </xsl:template>\n" +
                "\n" +
                "  <xsl:template match=\"Author\">\n" +
                "    - <xsl:value-of select=\".\" />\n" +
                "  </xsl:template>\n" +
                "\n" +
                "</xsl:stylesheet>\n";

        System.out.println("classpath is " + System.getProperty("java.class.path"));

        URL url = getClass().getClassLoader().getResource("xslt_test.xsl");
         System.out.println("url is" + url.toExternalForm());

        StructType schema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("a", StringType, true)));
        List<Row> rows = Arrays.asList(RowFactory.create(xmlS));
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.show();
        df.printSchema();

        df.createOrReplaceTempView("books");
        SQLContext sqlContext = df.sqlContext();
        Dataset<Row> df2 = sqlContext.sql("select xslt(a,'" + url.toExternalForm() + "') from books");
        df2.show();

        /*
        multiply test
        String filename = "data/tuple-data-file.csv";
        Dataset<Row> df = spark.read().format("csv").option("inferSchema", "true")
                .option("header", "false").load(filename).toDF("a", "b");

        df.show();
        df.printSchema();
        df.createOrReplaceTempView("vals");


        SQLContext sqlContext = df.sqlContext();
        Dataset<Row> df2 = sqlContext.sql("select a as label, x2Multiplier(b) as value from vals");
        df2.show();
*/
    /*
        df = df.withColumn("label", df.col("_c0")).drop("_c0");
        df = df.withColumn("value", df.col("_c1")).drop("_c1");
        df = df.withColumn("x2", callUDF("x2Multiplier", df.col("value").cast(DataTypes.IntegerType)));
        df.show();
        */
    }
}
