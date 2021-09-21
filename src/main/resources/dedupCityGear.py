import datetime
import json

## This file should be registered as a gear with the redis instance/cluster involved
## use the RedisInsight tool for simple upload, activation, and monitoring
## https://redislabs.com/redis-enterprise/redis-insight/

# This gear is registered to listen for events on the stream: X:GBG:CITY
# these stream events are expected to contain strings containing a proposed city name - misspelled and / or wrong
# 1) 1621846248588-0
#     2) 1) spellCheckMe
#        2) burnabee
#        3) requestID
#        4) PM_UID75439582505275
# this gear
# 1. checks to see if the provided city-word has been checked before using the Redis Set data type (Set name is determined by first 2 letters in argument)
# 2. writes unique and unchecked city names to a targeted Stream for processing X:FOR_PROCESSING{routeValue}
# stream data looks like this:
# {\"key\": \"X:GBG:CITY{groupA}\", \"id\": \"1621833630358-0\", \"value\": {\"requestID\": \"PM_UID4\", \"spellCheckMe\": \"tauranto\"}}
# A couple of lua scripts to generate bogus city names and trigger this gear could look like this:

# EVAL "for index = 1,10000,1 do redis.call('SADD',KEYS[1],(string.char((index%20)+97)) .. string.char(((index+7)%11)+97) .. string.char(((index+3)%11)+97) .. 'SomeCity' .. index ) end" 1 s:cityList{groupA}
# EVAL "for index = 1,10000,1 do redis.call('XADD',KEYS[2],'*','requestID',index,'spellCheckMe',redis.call('SRANDMEMBER',KEYS[1])) end" 2 s:cityList{groupA} X:GBG:CITY{groupA}
# to test with the addressSearch Java Program:
##  delete one of the Set data structures indicating previously seen city name requests: s:addr:hits:{gS}
## and delete one of the Streams indicating work to be processed X:FOR_PROCESSING{gS}
## then make sure this gear is registered
## fire off the Main Java program to start the Stream processor
## run this LUA script:
# EVAL "for index = 1,10000,1 do redis.call('XADD',KEYS[2],'*','requestID',index,'spellCheckMe',redis.call('SRANDMEMBER',KEYS[1])) end" 2 s:cityList{groupA} X:GBG:CITY{groupA}

def cleanGarbage(starttime,s1):
  jsonVersion =json.loads(s1)
  cityName = jsonVersion['value']['spellCheckMe']
  requestID = jsonVersion['value']['requestID']
  execute('XADD', 'X:GEAR_DEDUPER_LOG', '*','RECEIVED--> '+s1+' cityName: '+cityName+' requestID: '+requestID)
  routingPrefix = cityName[0]+cityName[1]+cityName[2]
  if execute('SISMEMBER','s:addr:hits:{'+routingPrefix+'}',cityName) == 0:
    execute('SADD','s:addr:hits:{'+routingPrefix+'}',cityName)
    duration = datetime.datetime.now()-starttime
    execute('XADD','X:FOR_PROCESSING{'+routingPrefix+'}','*','garbageIn',cityName,'processTimeDuration: ',duration)
  else:
    a = 1
    #do nothing

# GB is the GearBuilder (a factory for gears)
s_gear = GB('StreamReader',desc = "When a bit of garbage (bad city name) comes in - search for something similar and publish the clean result" )
#class GearsBuilder('StreamReader').run(prefix='*', batch=1, duration=0, onFailedPolicy='continue', onFailedRetryInterval=1, trimStream=True)
s_gear.foreach(
  lambda x: cleanGarbage(datetime.datetime.now(),json.dumps(x))
)

s_gear.register(
    'X:GBG:CITY{groupA}',
    trimStream=False
    # setting trimStream=True  causes this gear to remove events from the stream it listens to as it processes them
)
