package org.bobsbits.bobs.sparksql.udfs;

import org.apache.spark.sql.api.java.UDF2;
import org.bobsbits.bobs.sparksql.utils.XsltUtils;

public class xslt implements UDF2<String,String,String> {

        private static final long serialVersionUID = -1372447039252716846L;

        @Override
        public String call(String xml,String xslt) throws Exception {
            if(xml == null) return null;
	        return XsltUtils.xslt(xml,xslt);
        }

}
