import os
import random
import time
import threading 
import sys


def parameters():
	return (4, 1024*1024, 1000)


def build_exec(blocksize, count):
	filename = "test.%d" % (random.random() * 1000)
	return ("/bin/dd",["dd","if=/dev/zero","of=" + filename, "bs=%d" % blocksize, "count=%d" % count])
 

def launch_process(qty, execgen):
	pf = []
 	for i in range(0,qty):	
		(execname,params) = execgen()
		pid = os.fork()
		if pid > 0:
			pf.append(pid)
		else:
			os.execv(execname,params)
        return pf			


def wait_for_workers(pf):
        rcs = []
	for pid in pf:
		rc = os.waitpid(pid,0)
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
	

def collect(collector_list): 
	i = 0
	sys.stdout.write("collecting... ")
	try: 
		while (keep_collecting):
			write_sec(i)	
			data = map(lambda x:x(), collector_list)
			stats_collected.append( (time.time(), data ))
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

	 
def print_report(data_to_print):
	print "#    %15s %15s %13s" % ("wchar","syscw","write_bytes")
	for i in range(0, len(data_to_print.values()[0])):
		print "%4d %15s %15s %15s" % (i+1,fmtn(data_to_print['char_w'][i]),fmtn(data_to_print['sysc_w'][i]),fmtn(data_to_print['byte_w'][i]) )
	

def report():
	totstats = sum_stats()
	data_to_print = dict(map ( lambda x:(x,calc_sec_rate( totstats[x])), totstats ))
	print_report(data_to_print)




if __name__ == "__main__":
	keep_collecting = True
	stats_collected = []
	(qprocess, blocksize, count) = parameters()	
	(execname, params) = build_exec(blocksize, count)
	execgen = lambda :build_exec(blocksize, count)
	pf = launch_process(qprocess, execgen)
	thread = launch_collectors(pf)
	res = wait_for_workers(pf)
	keep_collecting = False
	report()


		
