package org.chiwooplatform.integration.cloudwatch.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

import org.chiwooplatform.integration.cloudwatch.kafka.handler.CountDownLatchHandler;

@Configuration
public class ConsumerConfiguration {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.template.default-topic:sampleTopic}")
    private String defaultTopic;

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> properties = new HashMap<>();
        properties.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        properties.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
        properties.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
        properties.put( ConsumerConfig.GROUP_ID_CONFIG, "myGroup" );
        // automatically reset the offset to the earliest offset
        properties.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );
        return properties;
    }

    @Bean
    public ConsumerFactory<?, ?> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>( consumerConfigs() );
    }

    @SuppressWarnings("unchecked")
    @Bean
    public ConcurrentMessageListenerContainer<String, String> kafkaListenerContainer() {
        ContainerProperties containerProps = new ContainerProperties( defaultTopic );
        return (ConcurrentMessageListenerContainer<String, String>) new ConcurrentMessageListenerContainer<>( consumerFactory(),
                                                                                                              containerProps );
    }

    @Bean
    public KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter() {
        KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter<>( kafkaListenerContainer() );
        kafkaMessageDrivenChannelAdapter.setOutputChannel( consumingChannel() );
        return kafkaMessageDrivenChannelAdapter;
    }

    //
    // ~ -----------------------------------
    @Bean
    public DirectChannel consumingChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "consumingChannel")
    public CountDownLatchHandler countDownLatchHandler() {
        return new CountDownLatchHandler();
    }
}
