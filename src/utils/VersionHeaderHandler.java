package utils;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;

import java.io.IOException;
import tukano.api.rest.RestShorts;

@Provider
public class VersionHeaderHandler implements ContainerResponseFilter, ContainerRequestFilter {
    @Override
    public void filter(ContainerRequestContext reqCtx) throws IOException {
        String value = reqCtx.getHeaderString(RestShorts.HEADER_VERSION);
        if( value != null && ! value.isEmpty()) {
            version.set( Long.valueOf( value ) );
        }
    }

    @Override
    public void filter(ContainerRequestContext reqCtx, ContainerResponseContext resCtx) throws IOException {
        var value = version.get();
        if( value != null ) {
            resCtx.getHeaders().add(RestShorts.HEADER_VERSION, Long.toString( value ));
        }
    }

    public static final ThreadLocal<Long> version = new ThreadLocal<>();
}
