package com.hammer.proiecttae.util;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

@Slf4j
@Component
public class XsltTransformer {

    private final TransformerFactory factory = TransformerFactory.newInstance();

    public String transform(String inputXml, String xsltClasspathResource, Map<String, String> params) {
        log.info("Starting XSLT transformation with stylesheet '{}'", xsltClasspathResource);
        log.debug("Input XML:\n{}", inputXml);

        try {
            InputStream xslt = getClass().getResourceAsStream(xsltClasspathResource);
            if (xslt == null) {
                throw new IllegalArgumentException("XSLT resource not found: " + xsltClasspathResource);
            }

            Transformer transformer = factory.newTransformer(new StreamSource(xslt));
            params.forEach(transformer::setParameter);

            StringWriter out = new StringWriter();
            transformer.transform(new StreamSource(new StringReader(inputXml)), new StreamResult(out));

            String result = out.toString();
            log.info("XSLT transformation completed successfully");
            log.debug("Transformed output:\n{}", result);
            return result;

        } catch (TransformerException e) {
            throw new RuntimeException("XSLT transformation failed [" + xsltClasspathResource + "]", e);
        }
    }
}
