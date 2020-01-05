package org.bobsbits.bobs.sparksql.utils;


//https://www.programcreek.com/java-api-examples/?code=jgperrin/net.jgp.labs.spark/net.jgp.labs.spark-master/src/main/java/net/jgp/labs/spark/l150_udf/BasicUdfFromTextFile.java



import java.io.*;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;


import java.io.ByteArrayOutputStream;
import java.util.Base64;
import org.xerial.snappy.Snappy;

/* commoon base implementation to be used within pojos, and hive, beam, and spark udfs

Note there are several outstanding problems to solve here.
1) how to log
2) how to handle null inputs
3) how to handle bad inputs that cause exceptions

Thoughts so far -
2) at this level, inputs should not be null; check nullness in the udf
3) bad inputs should throw exceptions, its up to the wrapper to decide what to do.


 */


public class XsltUtils implements Serializable {

    private static final long serialVersionUID = 3492970200940899011L;

    public static String xslt(String xml_string, String xslt_resource_url) throws Exception {

        //TODO - compare processors
        //TODO - can we cache transformers by resource path, and then clone them when we use them?
        //TODO - is there a more performant option for handling strings?

            InputStream is = new ByteArrayInputStream(xml_string.getBytes());
            StreamSource source = new StreamSource(is);
            StreamSource stylesource = new StreamSource(XsltUtils.class.getResourceAsStream(xslt_resource_url));
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer(stylesource);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            StreamResult result = new StreamResult(baos);

            transformer.transform(source, result);
            return baos.toString();

    }


}
