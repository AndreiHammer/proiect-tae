package com.hammer.proiecttae.util;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import lombok.extern.slf4j.Slf4j;

import java.io.StringReader;
import java.io.StringWriter;

@Slf4j
public final class XmlUtils {

    public static <T> String marshal(T object, Class<T> clazz) {
        try {
            JAXBContext ctx = JAXBContext.newInstance(clazz);
            Marshaller marshaller = ctx.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_ENCODING, "UTF-8");

            StringWriter sw = new StringWriter();
            marshaller.marshal(object, sw);

            String xml = sw.toString();
            log.debug("Marshalled {} to XML:\n{}", clazz.getSimpleName(), xml);
            return xml;

        } catch (JAXBException e) {
            throw new IllegalStateException("Failed to marshal " + clazz.getSimpleName(), e);
        }
    }

    public static <T> T unmarshal(String xml, Class<T> clazz) {
        try {
            JAXBContext ctx = JAXBContext.newInstance(clazz);
            Unmarshaller unmarshaller = ctx.createUnmarshaller();

            return clazz.cast(unmarshaller.unmarshal(new StringReader(xml)));

        } catch (JAXBException e) {
            throw new IllegalStateException("Failed to unmarshal to " + clazz.getSimpleName(), e);
        }
    }
}
