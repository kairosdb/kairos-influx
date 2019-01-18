package org.kairosdb.telegraf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.zip.GZIPInputStream;

@Provider
@Consumes(MediaType.TEXT_PLAIN)
public class GzipMessageBodyReader implements MessageBodyReader<InfluxdbMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(GzipMessageBodyReader.class);

//    @Override
//    public Object aroundReadFrom(ReaderInterceptorContext context)  throws IOException, WebApplicationException
//    {
//        if ("gzip".equals(context.getHeaders().get("Content-Encoding"))) {
//            InputStream originalInputStream = context.getInputStream();
//            context.setInputStream(new GZIPInputStream(originalInputStream));
//        }
//        return context.proceed();
//    }

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType)
    {
        logger.info("class = " + aClass.getName() + " type=" + type.getTypeName() + " mediaType = " + mediaType);
        return false;
    }

    @Override
    public InfluxdbMessage readFrom(Class<InfluxdbMessage> aClass, Type type, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> multivaluedMap, InputStream inputStream)
            throws IOException, WebApplicationException
    {
        return null;
    }
}