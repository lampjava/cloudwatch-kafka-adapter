package org.chiwooplatform.integration.cloudwatch.kafka.support;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.springframework.web.filter.GenericFilterBean;

import org.chiwooplatform.integration.cloudwatch.kafka.CloudwatchKafkaApplication;
import org.slf4j.MDC;

public class TransactionLoggingFilter
    extends GenericFilterBean {

    private static final String TXID = CloudwatchKafkaApplication.TXID;

    @Override
    public void doFilter( ServletRequest request, ServletResponse response, FilterChain chain )
        throws IOException, ServletException {
        String tXID = (String) request.getAttribute( TXID );
        if ( tXID == null ) {
            tXID = Utils.getTXID();
        }
        MDC.put( TXID, tXID );
        request.setAttribute( TXID, tXID );
        try {
            chain.doFilter( request, response );
        } finally {
            MDC.remove( TXID );
        }
    }
}
