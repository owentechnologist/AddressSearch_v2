package com.redislabs.sa.ot.addresssearch;


import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import io.redisearch.client.IndexDefinition;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: implement robust streaming Consumer Group solution in place of existing
 * RedisStreamAdapter.java
 *
 *  Sample run command:
 *  mvn compile exec:java -Dexec.args="ddk"
 *
 *  Look at the gear for comments on how to prepare redis with city names
 *  for many simple searches
 */
public class Main {
    static final String XMARK = "XXXXX";
    static final String addrIndex = "IDX:ADDRESSES";
    static final String DATA_UPDATES_STREAM = "X:FOR_PROCESSING{sgc}";
    static final String DATA_UPDATES_STREAM_BASE = "X:FOR_PROCESSING{";
    static Jedis connection = null;
    static final JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
    static Main main = null;

    //Pass in the preferred routing value to listen for changes to
    //examples: ddk  okg   pkg
    //look for keys starting with X:FOR_PROCESSING and use one of their routing values
    public static void main(String [] args){
        String dataUpdatesStreamTarget = DATA_UPDATES_STREAM;
        if(args.length>0){
            dataUpdatesStreamTarget = DATA_UPDATES_STREAM_BASE+args[0];
        }
        main = new Main();
        System.out.println("cleanupIDX: ");
        main.cleanupIDX();
        System.out.println("prepCities: ");
        main.prepCities();
        System.out.println("prepIDX: ");
        main.prepIndex();
        //main.runQueryScoreTest();
        main.runHelperQueryScoreTest();
        RedisStreamAdapter streamAdapter = new RedisStreamAdapter(dataUpdatesStreamTarget,jedisPool);
        TestStreamEventMapProcessor streamEventMapProcessor =new TestStreamEventMapProcessor();
        streamEventMapProcessor.setCallbackTarget(new SearchHelper());
        streamAdapter.listenToStream(streamEventMapProcessor);
    }

    void runHelperQueryScoreTest(){
        System.out.println("\n\n\n<< >> TRYING HELPER SEARCH Queries: ");
        SearchHelper helper = new SearchHelper();
        //1115:
        helper.addrSearch("San Francisco","94016","CA","USA");
        //1113:
        helper.addrSearch("San Francisco","94016","CA","");
        //1104:
        helper.addrSearch("San Francisco","94016","","USA");
        //1013:
        helper.addrSearch("San Francisco","","CA","USA");
        //?:
        helper.addrSearch("San Francisco","94016","CA","US");
        //1102:
        helper.addrSearch("San Francisco","","CA","");
        //1001:
        helper.addrSearch("San Francisco","","","");
        //1012:
        helper.addrSearch("San Francisco","","CA","");
    }

    void prepIndex(){
        Schema sc = new Schema()
                .addField(new Schema.TextField(AddressSchemaConstants.cityAttribute, 1000.0, false, false, false, "dm:en"))
                .addTextField(AddressSchemaConstants.zipAttribute,100.0)
                .addTextField(AddressSchemaConstants.stateAttribute, 10.0)
                .addTextField(AddressSchemaConstants.countryAttribute,1.0)
                .addTagField(AddressSchemaConstants.cityTagAttribute)
                .addTagField(AddressSchemaConstants.zipTagAttribute)
                .addTagField(AddressSchemaConstants.stateTagAttribute)
                .addTagField(AddressSchemaConstants.countryTagAttribute);
        IndexDefinition def = new IndexDefinition()
                .setPrefixes(new String[] {"addr:"});
        Client client = new Client(addrIndex,jedisPool);
        client.createIndex(sc, Client.IndexOptions.defaultOptions().setDefinition(def));
    }

    void prepCities() {
        Map<String, String> map = new HashMap<String, String>();
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            map.put(AddressSchemaConstants.cityTagAttribute, "SanFrancisco");
            map.put(AddressSchemaConstants.cityAttribute, "SanFrancisco");
            map.put(AddressSchemaConstants.stateTagAttribute, "CA");
            map.put(AddressSchemaConstants.stateAttribute, "CA");
            map.put(AddressSchemaConstants.countryTagAttribute, "USA");
            map.put(AddressSchemaConstants.countryAttribute, "USA");
            map.put(AddressSchemaConstants.zipTagAttribute, "94016");
            map.put(AddressSchemaConstants.zipAttribute, "94016");
            jedis.hset("addr:sfo", map);

            map.put(AddressSchemaConstants.cityTagAttribute, "SanJose");
            map.put(AddressSchemaConstants.cityAttribute, "SanJose");
            map.put(AddressSchemaConstants.stateTagAttribute, "CA");
            map.put(AddressSchemaConstants.stateAttribute, "CA");
            map.put(AddressSchemaConstants.countryTagAttribute, "USA");
            map.put(AddressSchemaConstants.countryAttribute, "USA");
            map.put(AddressSchemaConstants.zipTagAttribute, "94088");
            map.put(AddressSchemaConstants.zipAttribute, "94088");
            jedis.hset("addr:sj", map);

            map.put(AddressSchemaConstants.cityTagAttribute, "LosAngeles");
            map.put(AddressSchemaConstants.cityAttribute, "LosAngeles");
            map.put(AddressSchemaConstants.stateTagAttribute, "CA");
            map.put(AddressSchemaConstants.stateAttribute, "CA");
            map.put(AddressSchemaConstants.countryTagAttribute, "USA");
            map.put(AddressSchemaConstants.countryAttribute, "USA");
            map.put(AddressSchemaConstants.zipTagAttribute, "90001");
            map.put(AddressSchemaConstants.zipAttribute, "90001");
            jedis.hset("addr:la", map);

            map.put(AddressSchemaConstants.cityTagAttribute, "Albuquerque");
            map.put(AddressSchemaConstants.cityAttribute, "Albuquerque");
            map.put(AddressSchemaConstants.stateTagAttribute, "NM");
            map.put(AddressSchemaConstants.stateAttribute, "NM");
            map.put(AddressSchemaConstants.countryTagAttribute, "USA");
            map.put(AddressSchemaConstants.countryAttribute, "USA");
            map.put(AddressSchemaConstants.zipTagAttribute, "87101");
            map.put(AddressSchemaConstants.zipAttribute, "87101");
            jedis.hset("addr:abq", map);
            // note in the args to the script below an unused key is provided with
            // a routing value between the {} which is all redis needs
            // to ensure the script is fired in the correct shard
            // redis search does not need all the hashes to be in the same shard
            // the lua script does.
            // comment out the lua script and use the for loop below it
            // to write hashes to all the shards if you prefer
            //jedis.eval("for index = 1,10000,1 do redis.call('HSET','addr:{sgc}:'..index,'city',string.char((index%20)+97) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) ..'SomeCity'..index,'city_as_tag',string.char((index%20)+97) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) .. 'SomeCity'..index,'state_or_province','CA','country','USA','zip_codes_or_postal_codes',''.. ((index%500)*10)+(88999)) end" ,1, "s:cityList{gS}" );
            String lua = "for index = 1,10000,1 " +
                    "do redis.call(" +
                    "'HSET','addr:{sgc}:'..index," +
                    "'"+AddressSchemaConstants.cityAttribute+"',string.char((index%20)+97) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) ..'SomeCity'..index," +
                    "'"+AddressSchemaConstants.cityTagAttribute+"',string.char((index%20)+97) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) .. 'SomeCity'..index," +
                    "'"+AddressSchemaConstants.stateTagAttribute+"','CA'," +
                    "'"+AddressSchemaConstants.stateAttribute+"','CA'," +
                    "'"+AddressSchemaConstants.countryTagAttribute+"','USA'," +
                    "'"+AddressSchemaConstants.countryAttribute+"','USA'," +
                    "'"+AddressSchemaConstants.zipTagAttribute+"',''.. ((index%500)*10)+(88999)," +
                    "'"+AddressSchemaConstants.zipAttribute+"',''.. ((index%500)*10)+(88999))" +
                    " end";
            jedis.eval(lua,1, "s:cityList{gS}" );
            /*
            This loop will write address hashes to all shards.
            This is because no routing value is provided as part of the keyname
            This is done much slower than when using the LUA script above ^
            for(int x = 0;x<10000;x++){
                map.put("city", "gSomeCity"+x);
                map.put("city_as_tag", "gSomeCity"+x);
                map.put("state_or_province", "NM");
                map.put("country", "USA");
                map.put("zip_codes_or_postal_codes", ""+((x%500)*10)+(83023)));
                jedis.hset("addr:"+x, map);
            }*/
        } catch (Throwable t) {
            System.out.println("WARNING: ");
            t.printStackTrace();
        }finally{jedis.close();}
    }

    void cleanupIDX(){
        try{
            Client client = new Client(addrIndex,jedisPool);
            client.dropIndex();
        }catch(Throwable t){
            System.out.println("WARNING: ");
            t.printStackTrace();}
    }


}
