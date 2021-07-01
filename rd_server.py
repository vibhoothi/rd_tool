#!/usr/bin/env python3

import tornado.ioloop
import tornado.web
import os
import codecs
import json
import argparse
import sshslot
import threading
import time
import awsremote
import queue
from work import *
from utility import *
from time import sleep
import requests
import math
import subprocess
import scipy.optimize

config_dir = os.getenv("CONFIG_DIR", os.getcwd())
runs_dst_dir = os.getenv("RUNS_DST_DIR", os.path.join(os.getcwd(), "../runs"))
codecs_src_dir = os.getenv("CODECS_SRC_DIR", os.path.join(os.getcwd(), ".."))

video_sets_f = codecs.open(os.path.join(config_dir, 'sets.json'),'r',encoding='utf-8')
video_sets = json.load(video_sets_f)

machines = []
slots = []
free_slots = []
work_list = []
run_list = []
work_done = []
args = {}
scheduler_tasks = queue.Queue()


run_status_url = "http://awcy.mindfreeze.tk/run_status.json"
build_status_url = "http://awcy.mindfreeze.tk/build_job_queue.json"
job_status_url = "http://awcy.mindfreeze.tk/list.json"
current_run_status = []
sucessful_runs = False

def submit_new_run(run, filename, extra_options, k_value):
    # python3 ../submit_awcy_sig.py -prefix av1-directopt-vimeo-single-acbr-s9-baserun
    # -commit 4fb45ab04 -set "vimeo-corpus-10s-single" -nick mindfreeze -extra_options "--alm_k=1 --alm_step=0 --cpu-used=9"
    # -encoding_mode bitrate
    # Here we need to have the creation of work_items, abstracted based on create_rdwork
    work_items = []
    max_retries = 5
    global slots
    global free_slots
    global work_list 
    global work_done 
    global scheduler_tasks
    for q in sorted(run.quality, reverse = True):
        work = RDWork()
        work.run = run
        work.log = run.log
        work.quality = q
        # This is a unique_name for notify submit_new_run
        work.encoding_mode = 'new_lambdatune'
        work.runid = run.runid
        work.codec = run.codec
        work.bindir = run.bindir
        work.set = run.set
        work.filename = filename
        work.extra_options = extra_options
        work.suffix = '-' + str(k_value) + '-daala.out'
        work.k_value = str(k_value)
        work.lambdarunid = run.runid + '-'+ filename + '-' + str(k_value)
        if run.save_encode:
            work.no_delete = True
            if work.codec == 'av1' or work.codec == 'av1-rt' or work.codec == 'rav1e' or work.codec == 'svt-av1' or work.codec == 'av1-cbr' or work.codec == 'av1-cbr2':
                work.copy_back_files.append('.ivf')
            elif (len(work.codec) >= 3) and (work.codec[0:3] == 'av2'):
                work.copy_back_files.append('.obu')
            elif work.codec == 'xvc':
                work.copy_back_files.append('.xvc')
        work_items.append(work)
    # Returns new set of work_items for specified filename
    return work_items

def return_server_status():
    # TODO: Need to check if the current lambda_runid is completed.
    if not requests.get(run_status_url).json():
        return True     # Returns True if the server queue is empty
    return False


encode_run_counter = 0
bdrate_pair = {}
cost_pair = {}


def cost_function(x, run, filename):
   
    if return_server_status():
        k_value = round(x,2)
        k1 = (math.floor(new_k_value*1000)) // 1000
        k2 = (math.floor(new_k_value*1000)%1000) //100
        k3 = (math.floor(new_k_value*1000)%100) //10
        run_options = '--alm_k=' + \
            str(k1) + ' --alm_step=' + str(k2) + ' --alm_step2='+ str(k3) + ' --cpu-used=9'
        print("DEBUG: brent_x: ", x, ", k_value: ", k_value)
        # Saves lot of CPU cycles as Data will be redudant by checking
        #  bdrate with this logic of {k_value:bd_rate}
        # Cost_pair is having cost and k_values
        if k_value in cost_pair:
            return cost_pair[k_value]
        else:
            global encode_run_counter
            encode_run_counter += 1
            submit_new_run(run, filename, run_options, k_value)
            # TODO: Add check/wait condition to check if the encode job for this specific k is complete.
            sucessful_runs = poll_current_server(run.run_id)
            if sucessful_runs:
                metric_data = bdrate(
                        'runs/' +
                        run.runid +
                        '/' +
                        run.set +
                        '/' +
                        filename +
                        '-daala.out',
                        'runs/' +
                        run.runid +
                        '/' +
                        run.set +
                        '/' +
                        filename +
                        '-' +
                        str(k_value) +
                        '-daala.out',
                        None,
                        True)
                # Experimental COST Function
                metrics_index = [0, 2, 3, 7, 8, 9, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]
                base_cost = 0
                for index_no in metrics_index:
                    print(metric_data[index_no])
                    metrics_val = metric_data[index_no]
                    if metrics_val is None:
                        continue
                    base_cost += metrics_val
                bdrate_value = metric_data[17] # MS_SSIM(libvmaf)
                #bdrate_pair[k_value] = bdrate_value
                cost_pair[k_value] = base_cost
                print("DEBUG: bdrate_value : ", str(bdrate_value))
                print("DEBUG: base_cost    : ", base_cost)
                return base_cost

config = {
  'runs': runs_dst_dir,
  'codecs': codecs_src_dir,
}

def lookup_run_by_id(run_id):
    for run in run_list:
        if run.runid == run_id:
            return run
    return None

class SchedulerTask:
    def get(self):
        pass

class CancelHandler(tornado.web.RequestHandler):
    def get(self):
        global scheduler_tasks
        run_id = self.get_query_argument('run_id')
        task = CancelTask()
        task.run_id = run_id
        scheduler_tasks.put(task)
        self.write('ok')

class CancelTask(SchedulerTask):
    def __init__(self):
        self.run_id = None
    def run(self):
        global work_list
        global work_done
        run_id = self.run_id
        rd_print(None,'Cancelling '+run_id)
        run = lookup_run_by_id(run_id)
        if not run:
            rd_print(None,'Could not cancel '+run_id+'. run_id not found.')
            return
        run.cancel()
        for work in work_list[:]:
            if work.runid == run_id:
                work_list.remove(work)
                work_done.append(work)
            else:
                rd_print(None, work.runid)
        rd_print(None, len(work_list))

class RunSubmitHandler(tornado.web.RequestHandler):
    def get(self):
        global scheduler_tasks
        run_id = self.get_query_argument('run_id')
        task = SubmitTask()
        task.run_id = run_id
        scheduler_tasks.put(task)
        self.write('ok')

class SubmitTask(SchedulerTask):
    def __init__(self):
        self.run_id = None
    def run(self):
        global work_list
        global run_list
        run_id = self.run_id
        rundir = config['runs'] + '/' + run_id
        info_file_path = rundir + '/info.json'
        log_file_path = rundir + '/output.txt'
        info_file = open(info_file_path, 'r')
        log_file = open(log_file_path, 'a')
        info = json.load(info_file)
        run = RDRun(info['codec'])
        run.info = info
        run.runid = run_id
        run.rundir = config['runs'] + '/' + run_id
        run.log = log_file
        run.set = info['task']
        run.encoding_mode = info['encoding_mode']
        run.bindir = run.rundir + '/x86_64/'
        run.prefix = run.rundir + '/' + run.set
        try:
            os.mkdir(run.prefix)
        except FileExistsError:
            pass
        if 'qualities' in info:
          if info['qualities'] != '':
              run.quality = info['qualities'].split()
        if 'extra_options' in info:
          run.extra_options = info['extra_options']
        if 'save_encode' in info:
            if info['save_encode']:
                run.save_encode = True
        run.status = 'running'
        run.write_status()
        run_list.append(run)
        video_filenames = video_sets[run.set]['sources']
        run.set_type = video_sets[run.set].get('type', 'undef')
        run.work_items = create_rdwork(run, video_filenames)
        work_list.extend(run.work_items)
        if False:
            if 'ab_compare' in info:
                if info['ab_compare']:
                    abrun = ABRun(info['codec'])
                    abrun.runid = run_id
                    abrun.rundir = config['runs'] + '/' + run_id
                    abrun.log = log_file
                    abrun.set = info['task']
                    abrun.bindir = config['codecs'] + '/' + info['codec']
                    abrun.prefix = run.rundir + '/' + run.set
                    run_list.append(abrun)
                    abrun.work_items.extend(create_abwork(abrun, video_filenames))
                    work_list.extend(abrun.work_items)
                    pass

class WorkListHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps([w.get_name() for w in work_list]))

class RunStatusHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/json")
        runs = []
        for run in run_list:
            run_json = {}
            run_json['run_id'] = run.runid
            run_json['completed'] = 0
            run_json['total'] = 0
            run_json['info'] = run.info
            for work in run.work_items:
                run_json['total'] += 1
                if work.done:
                    run_json['completed'] += 1
            runs.append(run_json)
        self.write(json.dumps(runs))

class MachineUsageHandler(tornado.web.RequestHandler):
    def get(self):
        global machines
        machine_usage = []
        for machine in machines:
            machine_json = {}
            slot_in_use = []
            for slot in machine.slots:
                if slot.work:
                    slot_in_use.append(slot.work.get_name())
                elif slot.busy:
                    slot_in_use.append('busy with no work')
                else:
                    slot_in_use.append('None')
            machine_json['name'] = machine.get_name()
            machine_json['slots'] = slot_in_use
            machine_usage.append(machine_json)
        self.write(json.dumps(machine_usage))
        self.set_header("Content-Type", "application/json")

class FreeSlotsHandler(tornado.web.RequestHandler):
    def get(self):
        global free_slots
        slot_text = []
        for slot in free_slots:
            if slot.work:
                slot_text.append(slot.work.get_name())
            elif slot.busy:
                slot_text.append('busy with no work')
            else:
                slot_text.append('None')
        self.write(json.dumps(slot_text))

class ExecuteTick(tornado.web.RequestHandler):
    def get(self):
        scheduler_tick()
        self.write('ok')


def main():
    global free_slots
    global machines
    global slots
    global args
    parser = argparse.ArgumentParser(description='Run AWCY scheduler daemon.')
    parser.add_argument('-machineconf')
    parser.add_argument('-port',default=4000)
    parser.add_argument('-awsgroup', default='nonexistent_group')
    parser.add_argument('-max-machines', default=3, type=int)
    args = parser.parse_args()
    if args.machineconf:
        machineconf = json.load(open(args.machineconf, 'r'))
        for m in machineconf:
            machines.append(sshslot.Machine(m['host'],m['user'],m['cores'],m['work_root'],str(m['port']),m['media_path']))
        for machine in machines:
            slots.extend(machine.get_slots())
        free_slots.extend(reversed(slots))
    app = tornado.web.Application(
        [
            (r"/work_list.json", WorkListHandler),
            (r"/run_status.json", RunStatusHandler),
            (r"/machine_usage.json", MachineUsageHandler),
            (r"/free_slots.json", FreeSlotsHandler),
            (r"/submit", RunSubmitHandler),
            (r"/cancel", CancelHandler),
            (r"/execute_tick",ExecuteTick)
        ],
        static_path=os.path.join(os.path.dirname(__file__), "static"),
        xsrf_cookies=True,
        debug=False,
        )
    app.listen(args.port)
    ioloop = tornado.ioloop.IOLoop.current()
    if not args.machineconf:
        machine_allocator_tick()
    scheduler_tick()
    ioloop.start()

def machine_allocator_tick():
    global slots
    global free_slots
    global machines
    global work_list
    global run_list
    # start all machines if we don't have any but have work queued
    if len(work_list) and not len(machines):
        rd_print(None, "Starting machines.")
        #awsgroup.start_machines(args.max_machines, args.awsgroup)
    # stop all machines if nothing is running
    slots_busy = False
    for slot in slots:
        if slot.busy:
            slots_busy = True
    if not slots_busy and not len(work_list) and not len(run_list):
        rd_print(None, "Stopping all machines.")
        machines = []
        slots = []
        free_slots = []
        #awsremote.stop_machines(args.awsgroup)
    try:
        updated_machines = awsremote.get_machines(args.max_machines, args.awsgroup)
    except:
        tornado.ioloop.IOLoop.current().call_later(60,machine_allocator_tick)
        return
    print(updated_machines)
    for m in machines:
        matching = [um for um in updated_machines if um.host == m.host]
        if len(matching) == 0:
            rd_print(None, "Machine disappeared: " + m.get_name())
            for s in m.slots:
                slots.remove(s)
                try:
                    free_slots.remove(s)
                except:
                    pass
            machines.remove(m)
    for um in updated_machines:
        print(um, um.get_name())
        matching = [m for m in machines if m.host == um.host]
        if len(matching) == 0:
            rd_print(None, "Machine appeared: " + um.get_name())
            new_slots = um.get_slots()
            slots.extend(new_slots)
            free_slots.extend(new_slots)
            machines.append(um)
    tornado.ioloop.IOLoop.current().call_later(60,machine_allocator_tick)

def find_image_work(items, default = None):
    for work in items:
        if work.run.set_type == 'image':
            return work
    return default

def run_queued_tasks(scheduler_tasks):
     # run queued up tasks
    while not scheduler_tasks.empty():
        task = scheduler_tasks.get()
        try:
            task.run()
        except Exception as e:
            rd_print(None,e)
            rd_print(None,'Task failed.')

def find_completed_work(slots, work_list, work_done, free_slots):
    max_retries = 5
    # look for completed work
    for slot in slots:
        if slot.busy == False and slot.work != None:
            if slot.work.failed == False:
                slot.work.done = True
                try:
                    slot.work.write_results()
                except Exception as e:
                    rd_print(None, e)
                    rd_print('Failed to write results for work item',slot.work.get_name())
                work_done.append(slot.work)

                rd_print(slot.work.log,slot.work.get_name(),'finished.')
            elif slot.work.retries < max_retries and not slot.work.run.cancelled and (slot.p.p is None or not slot.p.p.returncode == 98):
                slot.work.retries += 1
                rd_print(slot.work.log,'Retrying work ',slot.work.get_name(),'...',slot.work.retries,'of',max_retries,'retries.')
                slot.work.failed = False
                work_list.insert(0, slot.work)
            else:
                slot.work.done = True
                work_done.append(slot.work)
                rd_print(slot.work.log,slot.work.get_name(),'given up on.')
            slot.clear_work()
            free_slots.append(slot)

def fill_empty_slots(slots, work_list, free_slots):
    # fill empty slots with new work
    if len(work_list) != 0:
        if len(free_slots) != 0:
            slot = free_slots.pop()
            work = work_list[0]
            # search for image work if there is only one slot available
            # allows prioritizing image runs without making scheduler the bottleneck
            if len(free_slots) == 0:
                try:
                    work = find_image_work(work_list, work)
                except Exception as e:
                    rd_print(None, e)
                    rd_print(None, 'Finding image work failed.')
            work_list.remove(work)
            rd_print(work.log,'Encoding',work.get_name(),'on',slot.machine.host)
            slot.start_work(work)

def find_completed_runs(run_list)
    # find runs where all work has been completed
    for run in run_list:
        done = True
        lambda_tune = False
        for work in run.work_items:
            if work.done == False:
                done = False
            if work.encoding_mode == 'lambdatune':
                lambda_tune = True
        if done:
            run_list.remove(run)
            try:
                run.reduce()
            except Exception as e:
                rd_print(run.log,e)
                rd_print(run.log,'Failed to run reduce step on '+run.runid)
            rd_print(run.log,'Finished '+run.runid)
            run.finish()


def scheduler_tick():
    global slots
    global free_slots
    global work_list
    global run_list
    global work_done
    global scheduler_tasks

    run_queued_tasks(scheduler_tasks)
    find_completed_work(slots, work_list, work_done, free_slots)
    fill_empty_slots(slots, work_list, free_slots)
    find_completed_runs(run_list)

    # TODO: Check if the default runs are completed, once it is completed,
    # If it is completed and set encoding_mode as 'new_lambdatune' and start
    # a loop with video_list where you update global work_list and get outside
    # to run it.
    if lambda_tune:
        work_list = work_items + work_list
        video_filenames = video_sets[run.set]['sources']
        for filename in video_filenames:
            rd_print("Doing optimisation for", filename)
            test_var = scipy.optimize.fminbound(cost_function, 0.1, 4, full_output=True, args=(run, filename))
            rd_print(test_var)
    tornado.ioloop.IOLoop.current().call_later(1,scheduler_tick)

if __name__ == "__main__":
    main()
