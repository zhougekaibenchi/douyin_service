#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2022/10/26 14:15
# @Author  : stce

import sys
import datetime
from __init__ import *
from pipeline import douyin_pipeline
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

scheduler = BlockingScheduler()
ftp_trigger = CronTrigger(hour=22, minute=30)

def main():

    scheduler.add_job(func=douyin_pipeline, trigger=ftp_trigger, kwargs={"env":sys.argv[1]}, #{'env':'test'}
                      name="数据同步", id='pipeline', max_instances=2, replace_existing=True)
    scheduler.add_listener(listener_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    try:
        scheduler.start()
        logger.info("定时服务启动成功")
    except:
        scheduler.shutdown()
        logger.error("定时任务启动失败")

def listener_event(event):

    job = scheduler.get_job(event.job_id)
    if not event.exception:
        logger.info(f'{job.name}任务执行成功')
    else:
        t = datetime.datetime.now() + datetime.timedelta(seconds=60)
        scheduler.reschedule_job(job.id, trigger='cron', hour=t.hour, minute=t.minute, second=t.second)
        logger.error(f'name={job.name}|trigger={job.trigger}|error_code={event.code}|exception=[{event.exception}]|'
                     f'traceback=[{event.traceback}]|time={event.scheduled_run_time}')

if __name__ == "__main__":
    main()