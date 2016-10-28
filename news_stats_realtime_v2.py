#coding:utf-8

from multiprocessing import Process, Manager, Lock
import sys, json, time, traceback, datetime, logging
from kafka import KafkaConsumer
from db import DAL
from db2 import DAL2

DB_HOST = "172.31.2.150"
DB_NAME = "opera_news"
TABLE_NAME = "news_profile"
DB_USER = "mysql"
DB_PWD = ""

TOPIC = "client-log-0628"
BROKERS = ["172.31.14.227:9092", "172.31.14.234:9092", "172.31.14.233:9092", "172.31.14.232:9092"]
ZOO_SERVERS = ["172.31.14.213:2181","172.31.14.212:2181","172.31.14.230:2181","172.31.14.229:2181","172.31.14.228:2181"]
#CONSUMER = KafkaConsumer(TOPIC, bootstrap_servers = BROKERS)
manager = Manager()
lock = Lock()

def get_two_dates_str():
    now_date = datetime.datetime.now()
    last_date = now_date-datetime.timedelta(days=1)
    return now_date.strftime("%Y%m%d"), last_date.strftime("%Y%m%d")

def write_to_db(click_cnt_map,impr_cnt_map,now_date_str,dal):
    entry_ids = set(click_cnt_map.keys() + impr_cnt_map.keys())
    for entry_id in entry_ids:
        impr = impr_cnt_map.get(entry_id,0)
        click = click_cnt_map(entry_id,0)
        try:
            insert_sql = "insert into news_stats_realtime(entry_id, impr, click, create_time, others, `date`) values ('%s', %s, %s, NOW(), '', '%s') on duplicate key update impr=impr+%s, click=click+%s;" % (entry_id, impr, click, now_date_str, impr, click)
            logging.info("insert_sql:%s", insert_sql)
            dal.execute(insert_sql)
        except Exception as e:
            logging.exception(e)
            continue

def handle_messgae(today_entry_impr_users,today_entry_click_users):
    myConsumer = KafkaConsumer(TOPIC, bootstrap_servers = BROKERS)
    dal = DAL(host=DB_HOST, user=DB_USER, passwd=DB_PWD, name=DB_NAME, connect_timeout=20, read_timeout=30)
    myEntry_impr_cnt_map = {}
    myEntry_click_cnt_map = {}
    for message in myConsumer:
        if not message or not message.value:
            continue
        now_date_str,last_date_str  =get_two_dates_str():
        try:
            obj = json.loads(message.value)
            user_device_id = obj.get('news_device_id',None)
            events = obj.get('events',None)
            if not user_device_id or not events:
                continue
            for event in events:
                news_entry_id = event.get('news_entry_id',None)
                event_type = event.get('event_type',None)
                ts = int(event.get('ts',0))
                local_ts = time.localtime(ts)
                date_str = time.strftime('%Y%m%d',local_ts)
                if not news_entry_id or not event_type or not date_str:
                    continue
                if event_type not in ['impression','click']:
                    continue
                if date_str != now_date_str:
                    continue
                with lock:
                    if last_date_str in today_entry_impr_users:
                        today_entry_impr_users.get(last_date_str,{}).clear()
                    if last_date_str in today_entry_click_users:
                        today_entry_click_users.get(last_date_str,{}).clear()
                    today_entry_impr_users.setdefault(notow_date_str,manager.dict())
                    today_entry_click_users.setdefault(now_date_str,manager.dict())
                    today_impr_users = today_entry_impr_users[now_date_str]
                    today_click_users = today_entry_click_users[now_date_str]
                    logging.info("entry_id:%s, device_id:%s, type:%s", news_entry_id, user_device_id, event_type)
                    today_click_users.setdefault(news_entry_id,manager.dict())
                    today_impr_users.setdefault(news_entry_id,manager.dict())
                    if event_type == 'impression':
                        if user_device_id in today_impr_users[news_entry_id]:
                            continue
                        today_impr_users[news_entry_id][user_device_id] = True
                        myEntry_impr_cnt_map[news_entry_id] = myEntry_impr_cnt_map.get(news_entry_id,0) + 1
                    elif event_type == 'click':
                        if user_device_id in today_click_users[news_entry_id]:
                            continue
                        today_click_users[news_entry_id][user_device_id] = True
                        myEntry_click_cnt_map[news_entry_id] = myEntry_click_cnt_map.get(news_entry_id,0) + 1
                if len(myEntry_click_cnt_map) + len(myEntry_impr_cnt_map) > 300:
                    write_to_db(myEntry_click_cnt_map,myEntry_impr_cnt_map,now_date_str,dal)
                    myEntry_impr_cnt_map.clear()
                    myEntry_click_cnt_map.clear()
            write_to_db(myEntry_click_cnt_map,myEntry_impr_cnt_map,now_date_str,dal)
        except Exception,ex:
            sys.stderr.write('error: %s, %s'%(ex.message,traceback.format_exc()))


class NewsStatsRealtimeConsumer(object):
    def __init__(self,parall):
        self.parall = parall
        self.today_entry_impr_users = manager.dict()
        self.today_entry_click_users = manager.dict()
    
    def block_listen(self):
        workers = [Process(target = handle_messgae,args = (self.today_entry_impr_users,self.today_entry_click_users)) for i in range(self.parall)]
        for worker in workers:
            worker.start()
        logging.info('Begin to wait for workers process to terminate!')
        for worker in workers:
            worker.join()

if __name__ == "__main__":
    logging.basicConfig(filename='news_stats_realtime_v2.log',level = logging.INFO,format = '%(asctime)s %(levelname)s %(message)s')
    logging.info("begin to run")
    consumer = NewsStatsRealtimeConsumer()
    consumer.block_listen()
    