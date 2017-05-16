#!/usr/bin/env python3
#coding: utf-8

# In[2]:

import os,sys,re,time,random,getopt,json


# In[3]:

def Usage():
    print( 'script usage:')
    print( '-h: help')
    print( '-v: print script version')
    print( '--Config: config file' )
    print( '--Tumor: Tumor sample (They should be tumor,PRL,tissue,plasma )')
    print( r'--Normal: Normal sample (They can be PBL or paracancer)')
    print( '--Prio: the priority of qsub,default value is -1000 ')
    print( '--Run_way: the run way of the pipline(monitor,directly,NA),defalut value is NA which means only generating script')
    print( '--Node: the node of the main monitor progress,defalut value is long.q@hw10')


# In[5]:

def Version():
    print('ctDNA_pipline.py v3.0')
    exit()


# In[6]:

def OutPut(o,a):
    #print('{0} {1} '.format(o,a))
    return(a)


# In[7]:

def Defalut_par():
    Pardic = {
    'Run_way' : 'NA',
    'Node' : 'long.q@hw10',
    'Tumor' : 'NA',
    'Normal' : 'NA',
    'Prio' : '-1000',
    'Update' : 'NA',
    'QcReport' : 'NA',
    'Search_sample' : 'no'
    }
    return(Pardic)


# In[8]:

def config_fun(Pardic):
    STEP = {}
    STEP_index = 0
    try:
        with open(Pardic['Config'],'r') as handle:
            #print("----------------------read config start--------------------")
            for line in handle:
                if re.compile("^#").findall(line): continue
                line = line.rstrip()
                if not line: continue
                line = re.split('[\t\s]{1,}',line)
                if line[0] in Pardic.keys():
                    Pardic[line[0]] = line[1]
                else:
                    if re.compile("^:").findall(line[0]):
                        line[0] = line[0].replace(":","")
                        STEP_index += 1
                        STEP.update({STEP_index:[line[0],line[1],line[2]]})            
                    else:
                        Pardic.update({line[0]:line[1]})
                    
    except:
        print("Can not open the config file!!!!",file=sys.stderr)
        exit()
    #print('config file is {0}'.format(Pardic))
    #print('step is {0}'.format(STEP))
    #print('--------------------read config done-------------------------------')
    return(Pardic,STEP)


# In[13]:

def PAR(argv):
    Pardic = Defalut_par()
    opts, args = getopt.getopt(argv[1:], 'hv', ['Tumor=', 'Normal=','Run_way=','Node=','Config=','Prio='])
    for o, a in opts:
        if o == '-h':
            Usage()
            exit(0)
        elif o == '-v':
            Version()
            exit(0)
        elif o == '--Tumor':
            Pardic['Tumor'] = OutPut(o,a)
        elif o == '--Normal':
            Pardic['Normal'] = OutPut(o,a)
        elif o == '--Prio':
            Pardic['Prio'] = OutPut(o,a)
        elif o == '--Run_way':
            Pardic['Run_way'] = OutPut(o,a)
        elif o == '--Config':
            Pardic['Config'] = OutPut(o,a)
        elif o == '--Node':
            Pardic['Node'] = OutPut(o,a)       
        else:
            print('unhandled option')
            exit(1)
    Pardic, STEP = config_fun(Pardic)
    if Pardic['Tumor'] == 'NA':
        print("There are no tumor sample!",file=sys.stderr)
        Usage()
        exit(0)
    if Pardic['Normal'] == 'NA':
        Pardic.update({'Paired':False})
    else:
        Pardic.update({'Paired':True})

    return(Pardic,STEP)

   
# In[16]:

def get_script(Pardic,STEP):
    
    T_log = os.path.join(Pardic['Outdir'],Pardic['Tumor'].split('_')[0],'log')
    T_script = os.path.join(Pardic['Outdir'],Pardic['Tumor'].split('_')[0],'script')
    os.system('mkdir -p {0} {1}'.format(T_log,T_script))

    SOFT_dir = Pardic['Soft_dir']
    INDIR = Pardic['Indir']
    OUTPUT = Pardic['Outdir']
    REF = Pardic['Ref']
 
    if Pardic['Paired']:
        N_log = os.path.join(Pardic['Outdir'],Pardic['Normal'].split('_')[0],'log')
        N_script = os.path.join(Pardic['Outdir'],Pardic['Normal'].split('_')[0],'script')
        os.system('mkdir -p {0} {1}'.format(N_log,N_script))

    for every_step in STEP:
        module , runway , Wscript = STEP[every_step]
        
        #module_pip = '-'.join([module,Pardic['Pip']])
        module_v = Pardic[module]
        THREAD = Pardic[module+'_th']
 
        if '_nocontrol' in module:
            module_pip = module.split('_')[0] + '-' + module_v.split('.')[0] + '.0'
            module_dir = os.path.join(Pardic['Script_dir'],module.split('_')[0],module_pip)           
        else:
            module_pip = module + '-' + module_v.split('.')[0] + '.0'
            module_dir = os.path.join(Pardic['Script_dir'],module,module_pip)

        module_sh = os.path.join(module_dir,module + '-' + module_v + '.sh')

        if 'tumor' in Wscript:
            SAMPLE_T = Pardic['Tumor']            
            script_T = os.path.join(T_script,'.'.join([Pardic['Tumor'].split('_')[0],module+'-'+Pardic[module],'script']))
            SAMPLE_N = Pardic['Normal']
            cmd = 'sh sh_function.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9}'.format(SAMPLE_T,SAMPLE_N,SOFT_dir,THREAD,module_dir,INDIR,OUTPUT,module_sh,script_T,REF)
            print(cmd)
            os.system(cmd)
        elif 'both' in Wscript:
            SAMPLE_N = Pardic['Normal']
            script_N = os.path.join(N_script,'.'.join([Pardic['Normal'].split('_')[0],module+'-'+Pardic[module],'script']))
            SAMPLE_T = Pardic['Tumor']
            cmd = 'sh sh_function.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9}'.format(SAMPLE_N,SAMPLE_T,SOFT_dir,THREAD,module_dir,INDIR,OUTPUT,module_sh,script_N,REF)
            print(cmd)
            os.system(cmd) 
            SAMPLE_T = Pardic['Tumor']
            script_T = os.path.join(T_script,'.'.join([Pardic['Tumor'].split('_')[0],module+'-'+Pardic[module],'script'])) 
            SAMPLE_N = Pardic['Normal']
            cmd = 'sh sh_function.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9}'.format(SAMPLE_T,SAMPLE_N,SOFT_dir,THREAD,module_dir,INDIR,OUTPUT,module_sh,script_T,REF) 
            print(cmd)
            os.system(cmd)
        elif 'normal' in Wscript:
            SAMPLE_N = Pardic['Normal']
            script_N = os.path.join(N_script,'.'.join([Pardic['Normal'].split('_')[0],module+'-'+Pardic[module],'script']))
            SAMPLE_T = Pardic['Tumor']
            cmd = 'sh sh_function.sh {0} {1} {2} {3} {4} {5} {6} {7} {8} {9}'.format(SAMPLE_T,SAMPLE_N,SOFT_dir,THREAD,module_dir,INDIR,OUTPUT,module_sh,script_N,REF)
            print(cmd)
            os.system(cmd)
        else:
            print("stepParallel is not incompleted",file=sys.stderr)
            exit(1)          

        

# In[15]:

def JOB_fun(JOBID_file):
    try:
        with open(JOBID_file) as handle:
            for i in handle:
                i = i.rstrip()
                break
    except:
        print("there is no JOBID file",file=sys.stderr)
    return i 
                

#the function is used to submit mission.
def qsub_fun(Pardic,TYPE,module,sample):
    log_dir = os.path.join(Pardic['Outdir'],Pardic[TYPE].split('_')[0],'log')
    Script = os.path.join(Pardic['Outdir'],Pardic[TYPE].split('_')[0],'script','.'.join([Pardic[TYPE].split('_')[0],module + '-' + Pardic[module],'script']))
    if Pardic['Update'] == 'yes':
        cmd = 'qsub_script4.py {log_dir} {module_script} {sample} {thread} {AnalysisCode} {prio} {Update}'.format(log_dir=log_dir,module_script=Script,sample=sample,thread=Pardic[module+'_th'],AnalysisCode=Pardic['AnalysisCode'][TYPE],prio=Pardic['Prio'],Update=Pardic['Update'])
        os.system(cmd)
        print("cmd is:",cmd)
    else:
        cmd = 'qsub_script4.py {log_dir} {module_script} {sample} {thread} {AnalysisCode} {prio} {Update}'.format(log_dir=log_dir,module_script=Script,sample=sample,thread=Pardic[module+'_th'],AnalysisCode='None',prio=Pardic['Prio'],Update=Pardic['Update'])
        os.system(cmd)
        print("cmd is:",cmd)

def get_JOBID(module,Pardic,TYPE,sample):
    file = os.path.join(Pardic['Outdir'],Pardic[TYPE].split('_')[0],'log','.'.join([Pardic[TYPE].split('_')[0],module+'-'+Pardic[module],'JOBID']))
    return file

def Wait(qid_es,wait_time):
    len_overlap = 1
    qid_es = qid_es.split(',')
    while (len_overlap > 0):
        time.sleep(wait_time)
        cmd = r"qstat|cut -d' ' -f 2"
        qstat = list(os.popen(cmd))
        qid_stat = [qid.strip() for qid in qstat[2:] ]
        overlap = list(set(qid_es) & set(qid_stat))
        len_overlap = len(overlap)

def JOBID_trans(JOBID):
    if JOBID.split(',')[0] == '':
        JOBID = ','.join(JOBID.split(',')[1:])
    return(JOBID)

def Monitor(module, runway ,sample,Pardic,JOBID,wait_time):
    if  sample == 'both':
        s1 = Pardic['Tumor'].split('_')[0]
        s2 = Pardic['Normal'].split('_')[0]
        qsub_fun(Pardic,'Tumor',module,s1)
        qsub_fun(Pardic,'Normal',module,s2)
        JOBID_file_T =  get_JOBID(module,Pardic,'Tumor',s1)
        JOBID_file_N =  get_JOBID(module,Pardic,'Normal',s2)
        JOBID = ','.join([JOBID,JOB_fun(JOBID_file_T),JOB_fun(JOBID_file_N)])
        JOBID = JOBID_trans(JOBID)
        if runway == 'wait':
            Wait(JOBID,wait_time)
            print('JOBID is:',JOBID)
            print('b w')
    elif sample == 'tumor':
        s1 = Pardic['Tumor'].split("_")[0]
        qsub_fun(Pardic,'Tumor',module,s1)
        JOBID_file_T =  get_JOBID(module,Pardic,'Tumor',s1)
        #print("----------------",JOBID_file_T)
        #exit()
        JOBID = ','.join([JOBID,JOB_fun(JOBID_file_T)])
        JOBID = JOBID_trans(JOBID)
        if runway == 'wait':
            Wait(JOBID,wait_time)
            print('JOBID is:',JOBID)
            print('t w')
    elif sample == 'normal':
        s2 = Pardic['Normal'].split("_")[0]
        qsub_fun(Pardic,'Normal',module,s2)
        JOBID_file_N =  get_JOBID(module,Pardic,'Normal',s2)
        JOBID = ','.join([JOBID,JOB_fun(JOBID_file_N)])
        JOBID = JOBID_trans(JOBID)
        if runway == 'wait':
            Wait(JOBID,wait_time)
            print('JOBID is:',JOBID)
            print('n w')
    else:
        print('stepParallel is not completed[tumor,normal,both]',file=sys.stderr)
        exit()
    return(JOBID)

# In[ ]:
def eopen(Dir,img):
    all = os.listdir(Dir)
    files = []

    for one in all:
        if re.findall(re.compile(img),one):
            files.append(one)
   
    if len(files) == 0:
        print("there is no file {0}".format(img),file=sys.stderr)
        exit()
    failed_num = 0
    for file in files:
        with open(os.path.join(Dir,file)) as handle:
            for line in handle:
                if re.findall(re.compile('failed'),line):
                    print("erro_step {0}".format(line.rstrip()))
                    failed_num += 1
    return failed_num

    

def check_file_failed(Pardic):
    Dir_T = os.path.join(Pardic['Outdir'],Pardic['Tumor'].split("_")[0],'log')
    img = '\.o$'
    failed_num = eopen(Dir_T,img)    
    if Pardic['Paired']:
        Dir_N = os.path.join(Pardic['Outdir'],Pardic['Normal'].split("_")[0],'log')
        failed_num = eopen(Dir_N,img)
    return failed_num

def main(argv):
    Pardic, STEP = PAR(argv)
    os.chdir(Pardic['Outdir'])
    wait_time = 10


########generate script#####
    print('------------get script start----------')
    get_script(Pardic,STEP)
    print('---------generated script done--------')

    if Pardic['Run_way'] != 'monitor' and Pardic['Run_way'] == 'NA':
        print("config is : {0}".format(Pardic))
        exit()
#######get analysisCode#####

    AnalysisCode = {}
    Pardic.update({'AnalysisCode': AnalysisCode})


########run program#####
    print('---------submit progress and run----------')
    JOBID = ''
    for step_index in list(range(1,len(STEP.keys())+1)):
        module, runway ,sample = STEP[step_index]
        JOBID = Monitor(module, runway ,sample,Pardic,JOBID,wait_time)
    
#######check the running status of every step and make qc report#####
    failed_num = check_file_failed(Pardic)
    if failed_num > 0:
        print('----------somestep is failed-----------')
    else:
        print('-------the main progress is done--------')    
        print('-------update status of the final anaysis step---- ')
        #update final analysis status
        if Pardic['Update'] == 'yes':
            nes = {'analysisCode':Pardic['AnalysisCode']['Tumor']}
            up = {'analysisStatus':'end'}
            update_save_table('analysis','cu',nes,up)
            print('Tumor AnalysisCode is : {0}'.format(Pardic['AnalysisCode']['Tumor']))
            if Pardic['Paired']:
                 nes = {'analysisCode':Pardic['AnalysisCode']['Normal']}
                 up = {'analysisStatus':'end'}
                 update_save_table('analysis','cu',nes,up)
                 print('Normal AnalysisCode is : {0}'.format(Pardic['AnalysisCode']['Normal']))
         
        #make QcReport
        if Pardic['QcReport'] == 'yes':
            print("---------------make qc report---------------------")
            qc_cmd = 'qc_check.py {Outdir} {sample}'.format(Outdir=Pardic['Outdir'],sample=Pardic['Tumor'])
            print(qc_cmd)
            os.system(qc_cmd)
            qc_report_cmd = 'make.qc.report.py {sample1} 1>qc_report.1log 2>qc_report.2log'.format(sample1=Pardic['Tumor'])
            print(qc_report_cmd)
            os.system(qc_report_cmd)
             
            if Pardic['Paired']:
                qc_cmd = 'qc_check.py {Outdir} {sample}'.format(Outdir=Pardic['Outdir'],sample=Pardic['Normal'])
                print(qc_cmd)
                os.system(qc_cmd)
                qc_report_cmd = 'make.qc.report.py {sample1} {sample2} 1>qc_report.1log 2>qc_report.2log'.format(sample1=Pardic['Tumor'],sample2=Pardic['Normal'])
                print(qc_report_cmd)
                os.system(qc_report_cmd)

# In[ ]:

if __name__ == '__main__':
    main(sys.argv)
