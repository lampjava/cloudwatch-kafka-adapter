package org.chiwooplatform.integration.cloudwatch.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName("Notification")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
public class Notification {

    @JsonProperty("Type")
    private String Type;

    private String MessageId;

    private String TopicArn;

    private String Subject;

    private String message;

    private String Timestamp;

    private String SignatureVersion;

    private String Signature;

    private String SigningCertURL;

    private String UnsubscribeURL;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append( "Notification [Type=" );
        builder.append( Type );
        builder.append( ", MessageId=" );
        builder.append( MessageId );
        builder.append( ", TopicArn=" );
        builder.append( TopicArn );
        builder.append( ", Subject=" );
        builder.append( Subject );
        builder.append( ", Message=" );
        builder.append( message );
        builder.append( ", Timestamp=" );
        builder.append( Timestamp );
        builder.append( ", SignatureVersion=" );
        builder.append( SignatureVersion );
        builder.append( ", Signature=" );
        builder.append( Signature );
        builder.append( ", SigningCertURL=" );
        builder.append( SigningCertURL );
        builder.append( ", UnsubscribeURL=" );
        builder.append( UnsubscribeURL );
        builder.append( "]" );
        return builder.toString();
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
        return message;
    }

    public void setMessage( String message ) {
        this.message = message;
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

    public String getUnsubscribeURL() {
        return UnsubscribeURL;
    }

    public void setUnsubscribeURL( String unsubscribeURL ) {
        UnsubscribeURL = unsubscribeURL;
    }
}
