import multiprocessing as mp,os
import time
import sys

def process_wrapper(filename,chunkStart, chunkSize):
    #print("processing chunk")
    with open(filename) as f:
        f.seek(chunkStart)
        lines = f.read(chunkSize).splitlines()
        i = 0
        for line in lines:
           
            #Count the number of email sent
            i= i + 1
            '''wait for half a second, Simulate send E-Mail function'''
            '''check if it is a correct Email'''
            #is_valid_email(line)
            #send_email(line)
            time.sleep(0.5)
            print(line + " sent")
            
        return i

# Split large data file into small chunks, size can be decided accordingly
# here chunk size is 5 MB
def chunkify(fname,size=1024*1024 * 5):
    #print(fname)
    fileEnd = os.path.getsize(fname)
    #print(fileEnd)
    with open(fname,'rb') as f:
        chunkEnd = f.tell()
        while True:
            chunkStart = chunkEnd
            f.seek(size,1)
            f.readline().decode('utf-8')
            chunkEnd = f.tell()            
            yield chunkStart, chunkEnd - chunkStart
            if chunkEnd > fileEnd:
                break

result_list = []
def log_result(result):
    # Log how many results were sent in each chunk
    result_list.append(result)

def mailer(filename):
    #print(filename)
    #init objects
    pool = mp.Pool(mp.cpu_count())    
    
    #create jobs
    for chunkStart,chunkSize in chunkify(filename):
        '''process_wrapper(chunkStart,chunkSize) '''        
        pool.apply_async(process_wrapper,(filename,chunkStart,chunkSize), callback = log_result)

   
   
    #clean up
    pool.close()
    pool.join()
    #prints total number of emails sent chunk wise
    print(result_list)
    
if __name__=="__main__":
    if len(sys.argv) > 1:        
        mailer(sys.argv[1])
    else:
        mailer("input.txt")
