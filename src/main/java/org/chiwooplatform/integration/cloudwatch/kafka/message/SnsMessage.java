package org.chiwooplatform.integration.cloudwatch.kafka.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;

/**
 * {@link http://docs.aws.amazon.com/sns/latest/dg/json-formats.html AWS SNS Notificaton Json Message
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@JsonRootName("snsMessage")
public class SnsMessage {

    private String Type;

    private String MessageId;

    private String Token;

    private String TopicArn;

    private String Subject;

    private String Message;

    private String Timestamp;

    private String SignatureVersion;

    private String Signature;

    private String SigningCertURL;

    private String SubscribeURL;

    private String UnsubscribeURL;

    @Override
    public String toString() {
        return String.format( "SnsMessage [Type=%s, MessageId=%s, Token=%s, TopicArn=%s, Subject=%s, Message=%s, Timestamp=%s, SignatureVersion=%s, Signature=%s, SigningCertURL=%s, SubscribeURL=%s, UnsubscribeURL=%s]",
                              Type, MessageId, Token, TopicArn, Subject, Message, Timestamp, SignatureVersion,
                              Signature, SigningCertURL, SubscribeURL, UnsubscribeURL );
    }

    public String getType() {
        return Type;
    }

    public void setType( String type ) {
        Type = type;
    }

    public String getMessageId() {
        return MessageId;
    }

    public void setMessageId( String messageId ) {
        MessageId = messageId;
    }

    public String getToken() {
        return Token;
    }

    public void setToken( String token ) {
        Token = token;
    }

    public String getTopicArn() {
        return TopicArn;
    }

    public void setTopicArn( String topicArn ) {
        TopicArn = topicArn;
    }

    public String getSubject() {
        return Subject;
    }

    public void setSubject( String subject ) {
        Subject = subject;
    }

    public String getMessage() {
        return Message;
    }

    public void setMessage( String message ) {
        Message = message;
    }

    public String getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp( String timestamp ) {
        Timestamp = timestamp;
    }

    public String getSignatureVersion() {
        return SignatureVersion;
    }

    public void setSignatureVersion( String signatureVersion ) {
        SignatureVersion = signatureVersion;
    }

    public String getSignature() {
        return Signature;
    }

    public void setSignature( String signature ) {
        Signature = signature;
    }

    public String getSigningCertURL() {
        return SigningCertURL;
    }

    public void setSigningCertURL( String signingCertURL ) {
        SigningCertURL = signingCertURL;
    }

    public String getSubscribeURL() {
        return SubscribeURL;
    }

    public void setSubscribeURL( String subscribeURL ) {
        SubscribeURL = subscribeURL;
    }

    public String getUnsubscribeURL() {
        return UnsubscribeURL;
    }

    public void setUnsubscribeURL( String unsubscribeURL ) {
        UnsubscribeURL = unsubscribeURL;
    }
}