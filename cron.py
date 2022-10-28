#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/26 14:15
# @Author  : stce
import datetime
import sys
from __init__ import *
from pipeline import douyin_data_updata
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.triggers.cron import CronTrigger

def main():
    global scheduler
    scheduler = BlockingScheduler()
    global ftp_trigger
    ftp_trigger = CronTrigger(hour=18, minute=0)
    scheduler.add_job(func=douyin_data_updata, trigger=ftp_trigger, kwargs=[sys.argv[1]], #{'env':'test'}
                      name="数据同步", id='pipeline', max_instances=1, replace_existing=True)
    scheduler.add_listener(listener_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    try:
        scheduler.start()
        logger.info("定时服务启动成功")
    except:
        scheduler.shutdown()
        logger.error("定时任务启动失败")

def listener_event(event):

    job = scheduler.get_job(event.job_id, trigger=ftp_trigger)
    if not event.exception:
        logger.info(f'{job.name}任务执行成功')
    else:
        t = datetime.datetime.now() + datetime.timedelta(seconds=60)
        scheduler.reschedule_job(job.id, trigger='cron', hour=t.hour, minute=t.minute, second=t.second)
        logger.error(f'name={job.name}|trigger={job.trigger}|error_code={event.code}|exception=[{event.exception}]|'
                     f'traceback=[{event.traceback}]|time={event.scheduled_run_time}')

if __name__ == "__main__":
    main()