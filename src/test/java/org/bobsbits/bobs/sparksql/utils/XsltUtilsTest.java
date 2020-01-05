package org.bobsbits.bobs.sparksql.utils;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class XsltUtilsTest {

    @Test
    void xslt() throws Exception {
        String xml_string = readResource("/xslt_test_input.xml");
        String expected = readResource("/xslt_test_output.txt");
        //TODO - get xslt from resource

        String xslt_resource = "/xslt_test.xsl";
        String test = XsltUtils.xslt(xml_string,xslt_resource);
        Files.write(Paths.get("/tmp/output.txt"), test.getBytes());
        System.out.println(test);

        assert (test.equals(expected));
    }

    String readResource(String resource) throws Exception {
        InputStream in = XsltUtilsTest.class.getResourceAsStream(resource);
        int ch;
        StringBuilder sb = new StringBuilder();
        while ((ch = in.read()) != -1)
            sb.append((char) ch);
        //in.reset();
        return sb.toString();
    }
}
