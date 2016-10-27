import sys, json, time, traceback, datetime, logging, multiprocessing
from pykafka import KafkaClient
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
CONSUMER = KafkaConsumer(TOPIC, bootstrap_servers = BROKERS)

def get_two_dates_str():
    now_date = datetime.datetime.now()
    last_date = now_date-datetime.timedelta(days=1)
    return now_date.strftime("%Y%m%d"), last_date.strftime("%Y%m%d")

class NewsStatsRealtimeConsumer(object):
    def __init__(self):
        self.last_update_ts = time.time()
        self.today_entry_impr_users = dict()
        self.today_entry_click_users = dict()
        self.entry_impr_cnt_map = dict()
        self.entry_click_cnt_map = dict()
        self.dal = DAL(host=DB_HOST, user=DB_USER, passwd=DB_PWD, name=DB_NAME, connect_timeout=20, read_timeout=30)

    def need_to_write_db(self):
        if time.time()-self.last_update_ts > 300:
            return True
        return False

    def write_into_db(self, now_date_str, last_date_str):
        entry_ids = set(self.entry_impr_cnt_map.keys() + self.entry_click_cnt_map.keys())
        for entry_id in entry_ids:
            impr = self.entry_impr_cnt_map.get(entry_id, 0)
            click = self.entry_click_cnt_map.get(entry_id, 0)
            try:
                insert_sql = "insert into news_stats_realtime(entry_id, impr, click, create_time, others, `date`) values ('%s', %s, %s, NOW(), '', '%s') on duplicate key update impr=impr+%s, click=click+%s;" % (entry_id, impr, click, now_date_str, impr, click)
                logging.info("insert_sql:%s", insert_sql)
                self.dal.execute(insert_sql)
            except Exception as e:
                logging.exception(e)
                continue
        if last_date_str in self.today_entry_impr_users:
            self.today_entry_impr_users.get(last_date_str, {}).clear()
            self.today_entry_impr_users.pop(last_date_str)
        if last_date_str in self.today_entry_click_users:
            self.today_entry_click_users.get(last_date_str, {}).clear()
            self.today_entry_click_users.pop(last_date_str)
        self.entry_impr_cnt_map.clear()
        self.entry_click_cnt_map.clear()
        self.last_update_ts = time.time()

    def block_listen(self):
        for message in CONSUMER:
            if not message or not message.value:
                continue
            now_date_str, last_date_str = get_two_dates_str()
            if (self.need_to_write_db()):
                self.write_into_db(now_date_str, last_date_str)
            try:
                obj = json.loads(message.value)
                user_device_id = obj.get('news_device_id', None)
                events = obj.get('events', None)
                if not user_device_id or not events:
                    continue
                for event in events:
                    news_entry_id = event.get('news_entry_id', None)
                    event_type = event.get('event_type', None)
                    ts = int(event.get('ts', '0'))
                    local_ts = time.localtime(ts)
                    date_str = time.strftime('%Y%m%d', local_ts)

                    if not news_entry_id or not event_type or not date_str:
                        continue
                    if event_type not in ["impression", "click"]:
                        continue
                    if date_str != now_date_str:
                        continue

                    self.today_entry_impr_users.setdefault(now_date_str, {})
                    today_impr_users = self.today_entry_impr_users[now_date_str]
                    self.today_entry_click_users.setdefault(now_date_str, {})
                    today_click_users = self.today_entry_impr_users[now_date_str]

                    logging.info("entry_id:%s, device_id:%s, type:%s", news_entry_id, user_device_id, event_type)
                    today_impr_users.setdefault(news_entry_id, set())
                    today_click_users.setdefault(news_entry_id, set())
                    self.entry_impr_cnt_map.setdefault(news_entry_id, 0)
                    self.entry_click_cnt_map.setdefault(news_entry_id, 0)
                    if event_type == "impression":
                        if user_device_id in today_impr_users[news_entry_id]:
                            continue
                        today_impr_users[news_entry_id].add(user_device_id)
                        self.entry_impr_cnt_map[news_entry_id] = self.entry_impr_cnt_map.get(news_entry_id, 0) + 1
                    elif event_type == "click":
                        if user_device_id in today_click_users[news_entry_id]:
                            continue
                        today_click_users[news_entry_id].add(user_device_id)
                        self.entry_click_cnt_map[news_entry_id] = self.entry_click_cnt_map.get(news_entry_id, 0) + 1
            except Exception, ex:
                sys.stderr.write('error: %s, %s' % 
                     (ex.message, traceback.format_exc()))

if __name__ == "__main__":
    logging.basicConfig(filename='news_stats_realtime.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logging.info("begin to run")
    consumer = NewsStatsRealtimeConsumer()
    consumer.block_listen()
    time.sleep(50)
