import threading
import multiprocessing
import queue
from tqdm import tqdm_notebook
import time

class CheckerThread(threading.Thread):
    def __init__(self, q_done, expc, th_list, name=None):
        super(CheckerThread, self).__init__()
        self.name = name
        self.kill_me = False
        self.th_list = th_list
        self.q_done = q_done
        self.expc = expc
        return

    def run(self):
        last = 0
        with tqdm_notebook(total=self.expc) as tq:
            while self.q_done.qsize() < self.expc and self.kill_me is False:
                new = self.q_done.qsize()
                tq.update(new - last)
                time.sleep(1)
                last = new
            if self.q_done.qsize() == self.expc:
                tq.update(self.expc - last)
        for t in self.th_list:
            t.stop()
        return

    def stop(self):
        for t in self.th_list:
            t.stop()
        self.kill_me = True


class ConsumerProducerThread(threading.Thread):
    def __init__(self, q_todo, q_done, expc, function, name=None):
        super(ConsumerProducerThread, self).__init__()
        self.name = name
        self.kill_me = False
        self.q_todo = q_todo
        self.q_done = q_done
        self.function = function
        self.expc = expc
        return
        
    def logic(self):
        if not self.q_todo.empty():
            item = self.q_todo.get()
            is_ok, output = self.function(item)
            if not is_ok:
                self.q_todo.put(item)
            else:
                self.q_done.put([item, output])

    def run(self):
        while self.q_done.qsize() < self.expc and self.kill_me is False:
            self.logic()
        return

    def stop(self):
        self.kill_me = True


def multiqueue(input_list, function, n_cores=None, expc=None):
    if n_cores is None:
        cores = multiprocessing.cpu_count()
        n_cores = max(1, cores-1)

    if expc is None:
        expc = len(input_list)
    
    q_todo = queue.Queue()
    q_done = queue.Queue()
    for i in input_list:
        q_todo.put(i) 
    
    th = [ConsumerProducerThread(q_todo, q_done, expc, function, name='th' + str(i)) for i in range(n_cores)]
    for t in th:
        t.start()

    ch = CheckerThread(q_done, expc, th, name="th" + str(n_cores))
    ch.start()
    try:
        ch.join()
    except (Exception, KeyboardInterrupt) as e: 
        for t in th:
            t.stop()
        print(e)
        
    return list(q_done.queue)