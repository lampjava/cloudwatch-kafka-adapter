package org.chiwooplatform.integration.cloudwatch.message;

import org.springframework.web.client.RestTemplate;

public class Test {

    @org.junit.Test
    public void testName()
        throws Exception {
        RestTemplate t = new RestTemplate();
        Runnable r = new Runnable() {

            @Override
            public void run() {
                for ( int i = 0; i < 2000; i++ ) {
                    long timestamp = System.currentTimeMillis();
                    String s = t.getForObject( "http://ec2-52-79-190-59.ap-northeast-2.compute.amazonaws.com:8085/adapter/cloudwatch/v1/feedback?ksdhksdhslkdhsadlkhsalkhsadl"
                        + timestamp, String.class );
                    System.out.println( "s: " + i + "   " + s );
                }
            }
        };
        Thread t1 = new Thread( r );
        Thread t2 = new Thread( r );
        Thread t3 = new Thread( r );
        t1.start();
        t2.start();
        t3.start();
        boolean running = true;
        while ( running ) {
            if ( !t1.isAlive() && !t2.isAlive() && !t3.isAlive() ) {
                running = false;
            }
        }
    }
}
