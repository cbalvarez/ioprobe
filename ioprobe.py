import os
import random
import time
import threading 
import sys
import signal
import getopt
import re
import ast


def params_fail():
	print "usage: python ioproble.py -w <# of workers> -b <block size (k|M)?> -w <#blocks to write per worker> \n\n"
	exit(1)
	
def check_param(param, regex):
	if (re.match(regex, param) == None):
		params_fail()

def check_param_list(param, regex):
	list = ast.literal_eval(param)
	for l in list:
		if (re.match(regex, l) == None):
			params_fail()	

def process_params(workers, blocksize, writecount, list):
	if (workers == "" or blocksize == "" or writecount == "" ):
		params_fail()
	check_param(workers,"[0-9]+$")
	check_param(blocksize,"[0-9]+(M|k)?")
	check_param(writecount,"[0-9]+$")
	check_param_list(list, "^(.*/)([^/]*)$")
	return (int(workers), blocksize, int(writecount), ast.literal_eval(list))


def parameters(argv):
	opts, args = getopt.getopt(argv,"w:b:c:d:",["workers=","blocksize=","writecount=","dirlist="])
	workers, blocksize, writecount, list = ("","","", "")
	for opt, arg in opts:
		if opt in ("--workers","-w"):
			workers = arg
		elif opt in ("--blocksize","-b"):
			blocksize = arg
		elif opt in ("--writecount","-c"):
			writecount = arg
		elif opt in ("--dirlist","-d"):
			list = arg
	return process_params(workers, blocksize, writecount, list)


def build_exec(blocksize, count, directory):
	filename = "test.%d" % (random.random() * 1000)
	return ("/bin/dd",["dd","if=/dev/zero","of=%s/%s" %(directory,filename), "bs=%s" % blocksize, "count=%d" % count, "oflag=direct"])
 

def launch_process(qty, execgen, directories):
	pf = []
	fd = open("/dev/null")
 	for i in range(0,qty):	
		(execname,params) = execgen(directories[i % len(directories)])
		pid = os.fork()
		if pid > 0:
			pf.append(pid)
		else:
			os.dup2(fd.fileno(), sys.stdout.fileno()) 
			os.dup2(fd.fileno(), sys.stderr.fileno()) 
			fd.close()
			os.execv(execname,params)
        return pf			


def wait_for_workers(pf):
        rcs = []
	for pid in pf:
		try: 
			rc = os.waitpid(pid,0)
		except KeyboardInterrupt:
			sys.exit(1)
		rcs.append(rc)
	return rcs	

		
def collect_data(pid,statfile):
        statfile.seek(0) 
	lines = statfile.readlines()
	char_w = lines[1][7:-1]
	sysc_w = lines[3][7:-1]
	byte_w = lines[5][13:-1]
	return {'char_w':char_w, 'sysc_w':sysc_w, 'byte_w':byte_w}

	
def collector(pid):
        fn = "/proc/%d/io" % pid
	f = open(fn)
	return lambda :(pid, collect_data(pid, f))


def write_sec(s):
	sec = str(s)
	sys.stdout.write(str(sec))
 	for i in range(0, len(sec)): sys.stdout.write("\b")	
	sys.stdout.flush()
	

def tot(name, stat):
	return (name,sum(map (lambda x:int(x[name]),stat)))


def group_stat(curr_stat):
	stat = map (lambda x:x[1], curr_stat[1])
 	return dict(map (lambda x:tot(x,stat), ['char_w','sysc_w','byte_w']))



def print_current(stats_collected, i):
	if (i >= 1):
		curr_tot = group_stat(stats_collected[i])
		prev_tot = group_stat(stats_collected[i-1])
		char_w = curr_tot["char_w"] - prev_tot["char_w"]
	 	sys_w = curr_tot["sysc_w"] - prev_tot["sysc_w"] 
		byte_w = curr_tot["byte_w"] - prev_tot["byte_w"]
		print "#%4d %15s %15s %15s" % (i, fmtn(char_w), fmtn(sys_w), fmtn(byte_w))
			
	   
	  

def collect(collector_list): 
	i = 0
	sys.stdout.write("generating load and measuring... \n")
	print "#    %15s %15s %13s" % ("wchar","syscw","write_bytes")
	try: 
		while (keep_collecting):
			data = map(lambda x:x(), collector_list)
			stats_collected.append( (time.time(), data ))
			print_current(stats_collected,i)	
			i=i+1
			time.sleep(1)
	except:
		return



def launch_collectors(pid_list):
        collector_list = map (lambda x:collector(x), pid_list)
	lambda_thread = lambda :collect(collector_list)
	t = threading.Thread(target = lambda_thread) 
	t.start()	
	return t



def sum_metric(metric_by_second,name):
	return sum(map(lambda x:int(x[1][name]), metric_by_second[1]))


def sum_stats():
       	 st_cw = map ( lambda mbs:sum_metric(mbs,'char_w'), stats_collected) 
       	 st_sw = map ( lambda mbs:sum_metric(mbs,'sysc_w'), stats_collected) 
       	 st_bw = map ( lambda mbs:sum_metric(mbs,'byte_w'), stats_collected) 
	 assert (len(st_cw) == len(st_sw))
	 assert (len(st_cw) == len(st_bw))
	 return {'char_w' : st_cw, 'sysc_w' : st_sw, 'byte_w' : st_bw}


def calc_sec_rate(totstats):
	res = []
 	for i in range(1,len(totstats)):	
		res.append( totstats[i] - totstats[i-1] )
	return res
	    

def fmtn(n):
	return "{:,}".format(n)

	 



if __name__ == "__main__":
	keep_collecting = True
	stats_collected = []
	(qprocess, blocksize, count, directories) = parameters(sys.argv[1:])	
	#(execname, params) = build_exec(blocksize, count)
	execgen = lambda x:build_exec(blocksize, count, x)
	pf = launch_process(qprocess, execgen, directories)
	thread = launch_collectors(pf)
	res = wait_for_workers(pf)
	keep_collecting = False


		
