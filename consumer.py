import threading, sys
import time
import random
import datetime
import json
#import pymongo
import pandas as pd
from pandas.io.json import json_normalize

from time import sleep
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from json import loads


# Global shut down variable to signal threads
shutdown = False
csv = pd.DataFrame(columns = ["metrics"])

write_to_mongo = False
write_csv = False

class consumer_thread (threading.Thread):
    def __init__(self, threadID, name, bootstrap_servers, topicName, partition_num):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.servers = bootstrap_servers
        self.topicName = topicName
        self.partition_num = partition_num
    def run(self):
        threadLock.acquire()
        messages.append("Starting " + self.name + " " + time.ctime(time.time()))
        threadLock.release()
        consume(self.name, self.servers, self.topicName, self.partition_num)

#class mongo_thread (threading.Thread):
#    def __init__(self, threadID, name, mongo_server, db_name, collection):
#        threading.Thread.__init__(self)
#        self.threadID = threadID
#        self.name = name
#        self.mongo_server = mongo_server
#        self.db_name = db_name
#        self.collection = collection
#        self.mongo_threads = []
#        self.max_mongo_threads = 6
#    def run(self):
#        message_queue = []
#        threadLock.acquire()
#        messages.append("Starting " + self.name + " " + time.ctime(time.time()))
#        threadLock.release()
#        while not shutdown:
#            time.sleep( 0.25 )
#            thread_count = self.cleanup_threads()
#            while len(data_messages) and (thread_count < self.max_mongo_threads):
#                mongo_lock.acquire()
#                message_queue.clear()
#                while len(data_messages) > 0:
#                    message_queue.append(data_messages.pop(0))
#                if len(message_queue) > 0:
#                    thread = mongo_insert_thread(thread_count + 1, "Thread-" + str(thread_count + 1), self.mongo_server, self.db_name, self.collection, message_queue)
#                    thread.daemon = True
#                    thread.start()
#                    self.mongo_threads.append(thread)
#                thread_count = self.cleanup_threads()
#                mongo_lock.release()
#    def cleanup_threads(self):
#        thread_count = 0
#        for t in self.mongo_threads:
#            if not t.is_alive():
#                t.handled = True
#            else:
#                thread_count += 1
#        self.mongo_threads = [t for t in self.mongo_threads if not t.handled]
#        return thread_count

#class mongo_insert_thread (threading.Thread):
#    def __init__(self, threadID, name, mongo_server, db_name, collection, data):
#        threading.Thread.__init__(self)
#        self.threadID = threadID
#        self.name = name
#        self.mongo_server = mongo_server
#        self.db_name = db_name
#        self.collection = collection
#        self.data = data
#        self.handled = False
#    def run(self):
#        mongo_insert_data(self.mongo_server, self.db_name, self.collection, self.data)

def consume(threadName, bootstrap_servers, topicName, partition_num):
    global shutdown

    # Set up consumer
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=1000)

    consumer.assign([TopicPartition(topicName, partition_num)])
    while True:
        for message in consumer:
            threadLock.acquire()
            recs = consumer.highwater(TopicPartition(topicName, partition_num))
            lag = recs - (message.offset + 1)
            messages.append("%s : %s : %d : %d of %d : lag=%d" % (threadName, message.topic, message.partition, message.offset + 1, recs, lag))
            threadLock.release()

#            if write_to_mongo:
#                mongo_lock.acquire()
#                data_messages.append(message.value)
#                mongo_lock.release()
        if shutdown:
#            if write_csv:
#                output_stats(threadName, topicName, partition_num, consumer.metrics(raw=False))
            messages.append('Signalling consumer close...')
            consumer.close(autocommit=True)
            break
    on_done(threadName)

def on_done(thrd):
    threadLock.acquire()
    messages.append("Exiting " + str(thrd) + " " + time.ctime(time.time()))
    threadLock.release()

def output_stats(thread_name, topic, partition, data):
    global csv
    dt = pd.json_normalize(data)
    df = pd.DataFrame(dt)
    df.insert(0, 'Partition', str(partition))
    df.insert(0, 'Topic', topic)
    df.insert(0, 'Thread', thread_name)
    df.drop(df.columns[len(df.columns)-1], axis=1, inplace=True)                #Drop last column which was [0] at time of writing
    tdf = df.transpose()
    threadLock.acquire()
    n = len(csv.index)
    if n == 0:
        csv = tdf.copy()
        csv.rename(columns={'Partition' + str(partition): csv.columns.values[0]}, inplace=True)
    else:
        csv = pd.concat([csv, tdf], axis=1)
    threadLock.release()

#def mongo_insert_data(mongo_server, db_name, collection, data):
#    mongo_client = pymongo.MongoClient(mongo_server)
#    mongo_db = mongo_client[db_name]
#    mongo_collection = mongo_db[collection]
#    x = mongo_collection.insert_many(data)
#    threadLock.acquire()
#    messages.append(str(len(data)) + " sent to mongoDB " + time.ctime(time.time()))
#    threadLock.release()


threadLock = threading.Lock()
threads = []
threadNum = -1
messages = []
bootstrap_servers = ['192.168.0.10:9092']
topicName = 'SensorReadings'

#mongo_lock = threading.Lock()
#mongodb_server = "mongodb://localhost:27017/"
#mongodb = "kafkatest"
#mongo_collection = "SensorReadings"

data_messages = []


consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

partition_nums = consumer.partitions_for_topic(topicName)

for partition_num in partition_nums:
    threadNum += 1
    thread = consumer_thread(threadNum, "Thread-" + str(threadNum), bootstrap_servers, topicName, partition_num)
    thread.daemon = True
    thread.start()
    threads.append(thread)

#if write_to_mongo:
#    threadNum += 1
#    thread = mongo_thread(threadNum, "MongoThread", mongodb_server, mongodb, mongo_collection)
#    thread.daemon = True
#    thread.start()
#    threads.append(thread)

time.sleep( 0.5 )
messages.append("Press Ctrl-C to shut down")

while True:
    try:
        while threading.active_count() > 1:
            time.sleep( 0.25 )
            while len(messages):
                threadLock.acquire()
                print(messages.pop(0))
                threadLock.release()
    except KeyboardInterrupt:
        messages.append( 'shutting down due to Ctrl-C..., work threads left: ' + str(len(threads))
        # trigger stop event for graceful shutdown
        isStillAlive = True
        while isStillAlive:
            isStillAlive = False
            for work_thread in threads:
                if work_thread.is_alive():
                    messages.append("Signalling work_thread to shut down...")
                    shutdown = True
                    time.sleep( 0.25 )
                    messages.append("work_thread " + str(work_thread) + ": killing...")
                    work_thread.join()
                    messages.append("work_thread " + str(work_thread) + ": ...pushing up daisies")
                else:
                    messages.append("work_thread " + str(work_thread) + " .. done!")
            while len(messages):
                threadLock.acquire()
                print(messages.pop(0))
                threadLock.release()
#        if write_csv:
#            csv.to_csv("ConsumerStats.csv", encoding='utf-8')
        sys.exit(1)
    except:
        sys.exit(1)

while len(messages):
    threadLock.acquire()
    print(messages.pop(0))
    threadLock.release()
#if write_csv:
#    csv.to_csv("ConsumerStats.csv", encoding='utf-8')