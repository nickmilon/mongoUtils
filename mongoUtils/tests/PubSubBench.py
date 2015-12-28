'''
Created on Jul 30, 2015

@author: milon
'''

import threading
import argparse
import datetime
from time import sleep
import random
from mongoUtils.pubsub import Sub, PubSub, MsgState, Acknowledge, PubSubStats
from mongoUtils.client import muClient
from mongoUtils.configuration import testDbConStr
from Hellas.Sparta import DotDot

def printif(*args):
    if __name__ == "__main__":
        print (args)


class PubSubController():
    _topics = ['red', 'green', 'blue']
    _verbs = ['paint', 'mix']
    _ackns = [Acknowledge.NO, Acknowledge.RECEIPT, Acknowledge.RESULTS]

    def __init__(self, collection_name, db, max_jobs=100):
        self._continue_jf = True
        self._continue_mon = True
        self.dt_start = datetime.datetime.utcnow()
        self.tailjobs_done = None
        self.tailjobs_posts = None
        self._collection_name = collection_name
        self._db = db
        self.pubsub = PubSub(collection_name, db=db, name='controller',
                             capped=True, reset=True, size=2 ** 30, max_docs=10 ** 6)

    @classmethod
    def _rnd_topic(cls):
        return cls._topics[random.randint(0, len(cls._topics)-1)]

    @classmethod
    def _rnd_verb(cls):
        return cls._verbs[random.randint(0, len(cls._verbs)-1)]

    @classmethod
    def _rnd_ackn(cls):
        return cls._ackns[random.randint(0, len(cls._ackns)-1)]

    def monitor(self, seconds=30):
        pubsubstats = PubSubStats(self.pubsub._collection)
        while self._continue_mon:
            seconds_run = (datetime.datetime.utcnow() - self.dt_start).total_seconds()
            if seconds_run >= seconds:
                self.stop()
            sleep(5)

        for i in pubsubstats.job_status(fields_list=['status.state'])():
            if 'status_state' in i['_id'].keys():
                i['_id']['status_state'] = MsgState.value_name(i['_id']['status_state'])
            printif(i)

        for i in pubsubstats.responce_stats()():
            printif(i)
            #{'seconds': 11.902, 'messages': 10000, 'msgPerSec': 840.1949252226517}
        printif("msgs: {msgs:4,d} seconds: {seconds:6,.2f} msgsPerSec: {msgsPerSec:6,.2f}".format(**pubsubstats.mesgs_persec()))
        return pubsubstats

    def job_factory(self, topic=None, verb=None, target=None, ackn=Acknowledge.RECEIPT, max_jobs=1000):
        cnt = 0
        while self._continue_jf and (max_jobs is None or cnt < max_jobs):
            cnt += 1
            r = self.pubsub.pub(payload={'cnt': cnt},
                                topic=self._rnd_topic() if topic is None else topic,
                                verb=self._rnd_verb() if verb is None else verb,
                                target=target,  ackn=self._rnd_ackn() if ackn is None else ackn)
            sleep(0)

    def monitor_jobs_posts(self, topic=None, verb=None, target=None, start_from_last=True, max_jobs=None):
        printif("on start pending jobs{:10,}".format(self.pubsub._collection.count()))
        self.tailjobs_posts = self.pubsub.tail(topic=topic, verb=verb, target=None,
                                               projection=None, start_from_last=False, sleep_secs=0)
        cnt = 0
        for job in self.tailjobs_posts:
            cnt += 1
            if max_jobs is not None and cnt == max_jobs:
                self.pubsub.stop()
            if job['ackn'] == Acknowledge.RESULTS:
                self.pubsub.acknowledge_done(job, MsgState.SUCCES)
            sleep(0)
        self.pubsub.restart()

    def stop(self):
        printif("stopping now")
        self._continue_jf = False
        self._continue_mon = False
        sleep(5)
        self.pubsub.stop()
        printif("stopped")


def thread_start(target, name, daemon=True, args=(), kwargs={}):
    thr = threading.Thread(target=target, name=name, args=args, kwargs=kwargs)
    if daemon is True:
        thr.daemon = True
    thr.start()
    # threads_list.append(thr)
    return thr


def run(connection, collection_name, topic=None, verb=None, target=None, start_from_last=True, max_jobs=1000, ms=20):
    client = muClient(connection)
    assert client.db is not None
    pbs_controller = PubSubController(collection_name, client.db)
    reckwargs = {'topic': topic, 'verb': verb, 'target': target}
    thread_start(pbs_controller.monitor_jobs_posts, 'jmonitor_jobs_posts', daemon=True, kwargs=reckwargs)
    thread_start(pbs_controller.job_factory, 'job_factory', daemon=True, kwargs={'max_jobs': max_jobs})
    return pbs_controller.monitor(ms)


def ps_tests(testname, connection, collection_name='PubSubSimulate'):
    def speed():
        """non threading, tests for speed checks both writing / reading speed 
        """
        max_jobs = 10000
        res = DotDot()
        client = muClient(connection)
        pbs_controller = PubSubController(collection_name, client.db)
        dt_start_pub = datetime.datetime.utcnow()
        pbs_controller.job_factory(topic='foo', verb='bar', target=None,  max_jobs=max_jobs)
        dt_start_sub = datetime.datetime.utcnow()
        pbs_controller.monitor_jobs_posts(topic='foo', verb='bar', target=None, start_from_last=False, max_jobs=max_jobs)
        dt_end = datetime.datetime.utcnow()
        res.secondsToPub = (dt_start_sub-dt_start_pub).total_seconds()
        res.secondsToSub = (dt_end - dt_start_sub).total_seconds()
        res.msgsPerSecPub = int(max_jobs / res.secondsToPub)
        res.msgsPerSecSub = int(max_jobs / res.secondsToSub)
        printif(res)
        pbs_controller.pubsub.reset()
        return res

    def query():
        """non threading, test
        """
        max_jobs = 10000
        res = DotDot()
        client = muClient(connection)
        pbs_controller = PubSubController(collection_name, client.db)
        pbs_controller.job_factory(topic=None, verb=None, target=None,  max_jobs=max_jobs)
        pbs_controller.monitor_jobs_posts(topic='red', verb='paint', target=None, start_from_last=False, max_jobs=max_jobs)

   
        printif(res)
        pbs_controller.pubsub.reset()
        return res
    if testname == 'speed':
        return speed()
    elif testname == 'speedThread':
        res = run(connection, collection_name, topic=None, verb=None, target=None,
                  start_from_last=False, max_jobs=2000, ms=10)
        res = res.mesgs_persec()
        printif("res", res)
        return res
    elif testname == 'query':
        res = run(connection, collection_name, topic='red', verb='paint', target=None,
                  start_from_last=False, max_jobs=2000, ms=10)

def parse_args():
    parser = argparse.ArgumentParser(description="tail a mongodb collection")
    parser.add_argument('-connection', type=str, default=testDbConStr,
                        help='a db connection string\
                        as defined in http://docs.mongodb.org/manual/reference/connection-string/\
                        that included a database name defaults to testDbConStr in configuration file'
                        )
    parser.add_argument('-collection', type=str, help='collection name', default='PubSubSimulate')
    parser.add_argument('-topic', type=str, help='topic to subscribe', default=None)
    parser.add_argument('-verb', type=str, help='verb to subscribe', default=None)
    parser.add_argument('-target', type=str, help='target to subscribe', default=None)
    parser.add_argument('-max_jobs', type=int, help='max_number of Messages', default=1000)
    parser.add_argument('-test', type=str, help='test name', default=None,
                        choices=['speed', 'speedThread', 'query'])
    return parser.parse_args()


def main():
    args = parse_args()
    printif("starting PubSubBench", vars(args))
    if args.test is None:
        run(args.connection, args.collection, topic=args.topic, verb=args.verb, target=args.target, max_jobs=args.max_jobs)
    else:
        ps_tests(args.test, args.connection, args.collection)
 
if __name__ == "__main__":
    main()