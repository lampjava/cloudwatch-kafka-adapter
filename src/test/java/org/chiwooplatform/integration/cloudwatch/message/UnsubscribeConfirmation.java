package org.chiwooplatform.integration.cloudwatch.message;

public class UnsubscribeConfirmation {

    private String Type;

    private String MessageId;

    private String Token;

    private String TopicArn;

    private String Message;

    private String SubscribeURL;

    private String Timestamp;

    private String SignatureVersion;

    private String Signature;

    private String SigningCertURL;

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

    public String getMessage() {
        return Message;
    }

    public void setMessage( String message ) {
        Message = message;
    }

    public String getSubscribeURL() {
        return SubscribeURL;
    }

    public void setSubscribeURL( String subscribeURL ) {
        SubscribeURL = subscribeURL;
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
}
