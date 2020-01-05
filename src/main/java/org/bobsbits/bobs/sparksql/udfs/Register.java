package org.bobsbits.bobs.sparksql.udfs;

import java.io.Serializable;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;


public class Register implements Serializable {

    private static final long serialVersionUID = -6372447039252716846L;

    public static void register() {
        //TODO - should check tat there is an active session?
        //TODO - what should we do of there is no active session?

        SparkSession ss = SparkSession.active();
        ss.udf().register("xslt", new xslt(), DataTypes.StringType);
    }
}
