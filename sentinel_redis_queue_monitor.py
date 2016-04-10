###
##Anil Kuncham##
###
import redis
import traceback
import time
from Crypto import Random
from Crypto.Cipher import AES
from redis.sentinel import Sentinel
import threading
from redis.sentinel import Sentinel
import sys
import ConfigParser

BEAVER_REDIS_KEY = 'openstackbeaver'
FILE_PATH = '/etc/'
sentinel_port = 26379

def parseConfigFile():
    Config = ConfigParser.ConfigParser()
    Config.read("/etc/redis-monitor.conf")
    sections = Config.sections()
    dict1 = {}
    for section in sections:
        options = Config.options(section)
        for option in options:
            try:
                dict1[option] = Config.get(section, option)
                if dict1[option] == -1:
                    DebugPrint("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                dict1[option] = None
    return dict1


def get_redis_connection(sentinel_hosts,attempt,options):
    print 'Trying Sentinel host-'+str(attempt+1)+' '+sentinel_hosts[attempt]
    sentinel = Sentinel([(sentinel_hosts[attempt], sentinel_port)], socket_timeout=10)
    master = sentinel.master_for(options['cluster_name'], password=options['password'], socket_timeout=10)
    return master

def redis_queue_health_check():
    options = parseConfigFile()
    sentinel_password = options['password']
    sentinel_hosts = options['hosts']
    sentinel_hosts = sentinel_hosts.split(",")
    attempt = 0
    max_attempts = len(sentinel_hosts)
    if max_attempts==0:
        print 'Please provide atleast 1 sentinel instance'
    while attempt < max_attempts:
        redisobj = get_redis_connection(sentinel_hosts,attempt,options)
        try:
            queue_length = redisobj.llen(options['redis_key'])
            print 'length of queue: '+str(queue_length)
            break
        except redis.sentinel.MasterNotFoundError:
            pass
            print 'Connection to redis-sentinel is lost. Retrying'
        attempt = attempt+1
    if attempt == max_attempts:
        print 'Redis cluster is down'

def monitor_redis_queue_health():
    threading.Timer(5.0, redis_queue_health_check).start()

redis_queue_health_check()
