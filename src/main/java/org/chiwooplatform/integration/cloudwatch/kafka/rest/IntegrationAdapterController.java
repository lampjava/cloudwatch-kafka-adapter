package org.chiwooplatform.integration.cloudwatch.kafka.rest;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import org.chiwooplatform.integration.cloudwatch.kafka.CloudwatchKafkaApplication;
import org.chiwooplatform.integration.cloudwatch.kafka.message.SnsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

@RestController
public class IntegrationAdapterController {

    protected static final String BASE_URI = "/adapter/cloudwatch";


    private static final String TXID = CloudwatchKafkaApplication.TXID;
    
    protected final Logger logger = LoggerFactory.getLogger( this.getClass() );

    @Value("${spring.kafka.template.default-topic:sampleTopic}")
    private String defaultTopic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private RestTemplate restTemplate;

    private void send( final String key, final String message ) {
        // the KafkaTemplate provides asynchronous send methods returning a Future
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send( defaultTopic, key, message );
        // register a callback with the listener to receive the result of the send asynchronously
        future.addCallback( new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess( SendResult<String, String> result ) {
                logger.info( "sent message='{}' with topic={}, key={}, offset={}", message,
                             result.getProducerRecord().topic(), result.getProducerRecord().key(),
                             result.getRecordMetadata().offset() );
            }

            @Override
            public void onFailure( Throwable ex ) {
                logger.error( "unable to send message='{}'", message, ex );
            }
        } );
        // or, to block the sending thread to await the result, invoke the future's get() method
    }

    @RequestMapping(value = BASE_URI + "/{channelId}", method = RequestMethod.POST, produces = {
        MediaType.TEXT_PLAIN_VALUE })
    public void send( @PathVariable("channelId") String channelId, HttpServletRequest request ) {
        logger.info( " channelId: {}", channelId );
        
        String tXID = (String) request.getAttribute(  TXID );
        logger.info( "You can add TXID '{}' header-key of message for tracing requested transaction", tXID );
        
        Gson gson = new Gson();
        SnsMessage message = null;
        try {
            String payload = IOUtils.toString( request.getReader() );
            logger.info( "payload: {}", payload );
            message = gson.fromJson( payload, SnsMessage.class );
            logger.info( "message: {}", message );
            if ( "SubscriptionConfirmation".equals( message.getType() ) ) {
                String url = message.getSubscribeURL();
                logger.info( "Type: {}, SubscribeURL: {}", message.getType(), url );
                String response = restTemplate.getForObject( url, String.class );
                logger.info( "AWS-RESPONSE: {}", response );
            } else if ( "Notification".equals( message.getType() ) ) {
                logger.info( "Notify AWS-SNS Msg." );
                this.send( channelId, message.toString() );
            }
        } catch ( Exception e ) {
            logger.error( e.getMessage(), e );
        }
    }

    @RequestMapping(value = BASE_URI + "/feedback", method = RequestMethod.GET)
    public String feedback( HttpServletRequest request ) {
        return "ok, I've got it.";
    }
}
