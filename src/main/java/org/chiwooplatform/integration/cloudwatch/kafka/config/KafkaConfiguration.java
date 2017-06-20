package org.chiwooplatform.integration.cloudwatch.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@EnableKafka
@Configuration
public class KafkaConfiguration {

    private static final int CONNECTION_TIMEOUT = 3000;

    private ClientHttpRequestFactory getRequestFactory() {
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setReadTimeout( CONNECTION_TIMEOUT );
        factory.setConnectTimeout( CONNECTION_TIMEOUT );
        factory.setConnectionRequestTimeout( CONNECTION_TIMEOUT );
        return factory;
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate( getRequestFactory() );
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().enable( SerializationFeature.INDENT_OUTPUT )
                                 .enable( MapperFeature.USE_STD_BEAN_NAMING );
    }
}
