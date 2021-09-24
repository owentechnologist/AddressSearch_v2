package com.redislabs.sa.ot.addresssearch;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.StreamEventMapProcessor;
import io.redisearch.Document;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

import java.util.HashMap;
import java.util.Map;

public class TestStreamEventMapProcessor implements StreamEventMapProcessor {

    //make sure to set this value before passing this processor to the Stream Adapter
    private Object callbackTarget = null;

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        System.out.println("\nTestEventMapProcessor.processMap()>>\t"+payload.keySet());
        for( String se : payload.keySet()) {
            System.out.println(payload.get(se));
            StreamEntry x = payload.get(se);
            Map<String,String> m = x.getFields();
            String cityName = "";
            for( String f : m.keySet()){
                System.out.println("key\t"+f+"\tvalue\t"+m.get(f));
                if(f.equalsIgnoreCase("garbageIn")){
                    doSearch(m.get(f));
                }
            }
        }
    }

    //uses Main's addrSearch to fetch the document for the received Stream event
    // Not sure if we need to store the results anywhere...
    void doSearch(String city){
        long timestart = System.currentTimeMillis();
        Document addressFound = ((SearchHelper)callbackTarget).addrSearch(city,null,null,null);
        long timeend = System.currentTimeMillis();
        if(!(null==addressFound)) {
            Jedis jedis = null;
            try {
                jedis = JedisConnectionFactory.getInstance().getJedisPool().getResource();
                HashMap<String, String> map = new HashMap<>();
                map.put("arg_provided", city);
                map.put("score", ("" + addressFound.getScore()));
                map.put("searchTimeDurationMillis", "" + (timeend - timestart));
                map.put("docToString", addressFound.toString());

                jedis.xadd("X:searchResults", null, map);
            } catch (Throwable t) {
                System.out.println("WARNING:");
                t.printStackTrace();
            } finally {
                jedis.close();
            }
        }
    }

    public void setCallbackTarget(Object callbackTarget){
        this.callbackTarget = callbackTarget;
    }
}