package com.rmuti.twiterapiclient;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.io.UnsupportedEncodingException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import twitter4j.JSONException;
import twitter4j.JSONObject;

public class SampleStreamExample {

    public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException, UnsupportedEncodingException, JSONException {

        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        
        //StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        //endpoint.stallWarnings(false);
        
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("#ironman"));
        endpoint.stallWarnings(false);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
                .name("sampleExampleClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 100; msgRead++) {
            if (client.isDone()) {
                System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }

            String msg = queue.poll(5, TimeUnit.SECONDS);
            if (msg == null) {
                System.out.println("Did not receive a message in 5 seconds");
            } else {
                JSONObject json = new JSONObject(msg);
                int twiIndex = msgRead + 1;
                if (!json.isNull("text")) {
                    System.out.println("Msg " + twiIndex + " is : " + json.getString("text"));
                } else {
                    System.out.println("Msg " + twiIndex + " is empty ");
                }
            }
        }

        client.stop();

        // Print some stats
        System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
    }

    public static void main(String[] args) {
        try {
            SampleStreamExample.run(args[0], args[1], args[2], args[3]);
        } catch (InterruptedException e) {
            System.out.println(e);
        } catch (UnsupportedEncodingException ex) {
            System.out.println(ex);
        } catch (JSONException ex) {
            System.out.println(ex);
        }
    }
}
