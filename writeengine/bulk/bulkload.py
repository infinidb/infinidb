#!/usr/bin/python
##
## Bulkloader script by Martin Thomas
## 

import os, sys, glob, shutil, xml.dom.minidom
import getopt
import logging
import time

logger = logging.getLogger()
shdlr = logging.StreamHandler()
fhdlr = logging.FileHandler(filename='bulkload.log' )
formatter = logging.Formatter('%(asctime)s:%(levelname)s: %(message)s')
shdlr.setFormatter(formatter)
fhdlr.setFormatter(formatter)
logger.addHandler(shdlr)
logger.addHandler(fhdlr)

## only report INFO or higher - change to WARNING to silence all logging
logger.setLevel(logging.INFO)

    
def usage():
    print """
    
    Bulkload.py is intended to automate the manual steps required to load the database and build indexes from scratch.

    - ipcs-pat will be built if missing
    - cpimport will be removed and rebuilt
    - PrimProc will be stopped and started
    - shared memory sgements wil be removed using ipcs-pat
    - database files will be removed
    - dbgen will be run with option 5
    - oid files and job files will be copied to correct locations
    - column data will be parsed and loaded using Job 299
    - index data will be exported, sorted and loaded using Job 300

    Options:
      -w or --wedir=   : Specify the write engine branch to use instead of the default trunk
      -n or --nocache= : Specify either col or idx and the -c flag will NOT be sent to cpimport
      -u or --usage    : Usage message
      
    Example:
      bulkload.py -w/home/adevelop/genii/we1.1 --nocache=idx
      Load the database using the we1.1 branch for writeengine and do not use cache when building indexes
      
    THIS SPACE LEFT INTENTIONALLY BLANK
    """
    
def find_paths():
  
  """Find DBRoot and BulkRoot."""
  try:
    config_file = os.environ['CALPONT_CONFIG_FILE']
  except KeyError:
    try:
        logger.info("Environment variable CALPONT_CONFIG_FILE not set, looking for system Calpont.xml")
        config_file = '/usr/local/Calpont/etc/Calpont.xml'
        os.lstat(config_file)
    except:
        logger.error('No config file available')
        sys.exit('No config file available')
  try:
      xmldoc = xml.dom.minidom.parse(config_file)
      bulk_node = xmldoc.getElementsByTagName('BulkRoot')[0]
      db_node = xmldoc.getElementsByTagName('DBRoot1')[0]
      bulk_dir = bulk_node.childNodes[0].nodeValue
      data_dir = db_node.childNodes[0].nodeValue
      
  except Exception, e:
      logger.error('Error parsing config file')
      logger.error(e)
      sys.exit('Error parsing config file')

  return (bulk_dir, data_dir)

def check_dirs(bulkroot, dbroot):
    
    problem = 0
    res = 0
    reqd_dirs = {
    os.getenv('HOME')+'/genii' : "No genii directory found (contains tools required to continue) (%s)",
    bulkroot: "Bulkroot specified as %s but not found",
    bulkroot+'/job': "No job directory found - needed to store Job xml files (looked in %s)",
    bulkroot+'/data/import': "No data/import directory found - expected %s to hold data to be loaded",
    bulkroot+'/log': "No data/log directory found - expected %s to log into",
    dbroot : "DBroot specified as %s but not found"
    }
    for dir in reqd_dirs.keys():
        try:
            res = os.lstat(dir)
        except:
            problem = 1
            logger.error(reqd_dirs[dir]%dir)
            
    if problem:
        sys.exit(1)
            
def fix_hwm(job_file):
  
  """Find hwm in xml file and change to 0"""

  import re
  
  src_file = open(job_file, 'r')
  dst_file = open(job_file+'.tmp', 'w')

  rep = re.compile('hwm="1"')

  for line in src_file:
    line = rep.sub('hwm="0"', line)
    dst_file.write(line)
  # use os.rename instead of shutil.move to avoid problems traversing devices 
  os.rename(job_file+'.tmp', job_file)

def find_indexes(job_file):
  
  """Find index definitions in job_file and return list of files to sort"""

  index_files = []
  try: # try because we may have an old version of python
    xmldoc = xml.dom.minidom.parse(job_file)

    for index_node in xmldoc.getElementsByTagName('Index'):
      index_files.append(index_node.getAttribute('mapName'))
  except:
    import re
    f = open(job_file)
    for line in f.read():
      b =re.search('mapName="(CPL_[0-9A-Z_]+)"', line)
      try: # try because not every line will match
        index_files.append(b.group(1))
      except: pass
      
  return index_files

def exec_cmd(cmd, args):
  """Execute command using subprocess module or if that fails,
     use os.system
  """
  
  try:
    import subprocess

    try:
      retcode = call(cmd + " "+args, shell=True)
      if retcode < 0:
        print >>sys.stderr, "Child was terminated by signal", -retcode
        sys.exit(-1)

      else:
        print >>sys.stderr, "Child returned", retcode

    except OSError, e:

      print >>sys.stderr, "Execution failed:", e
      sys.exit(-1)
  except:
    logger.info ('Old version of Python - subprocess not available, falling back to os.system')
    logger.info ('Executing: '+cmd+' '+args)
    res = os.system(cmd+' '+args)
    if res:
      logger.error('Bad return code %i from %s'%(res, cmd))
      sys.exit( res )
             

def build_tool(tool):
  """
  Use the tool dictionary to determine if required tool exists
  and build if not
  """
  
  if not os.path.exists(tool['path']+tool['tool']):
    logger.warn ("Building %s before continuing"%tool['tool'])
    curdir=os.getcwd()
    os.chdir(tool['path'])
    exec_cmd(tool['builder'], tool['args'])
    os.chdir(curdir)

def main():
  """
  Bulk load the database..
  Check that we can write OIDfiles, that all required tools exist,
  clean up old files, sort the index inserts and generally rock and roll
  """
  start_dir = curdir=os.getcwd() # remember where we started
  
  if not os.access('.', os.W_OK):
    os.chdir('/tmp')
    logger.warn('Changing to /tmp to have permission to write files')

  if not os.environ.has_key('LD_LIBRARY_PATH'):
      logger.info('No environment variable LD_LIBRARY_PATH')
  else:
      if len(os.getenv('LD_LIBRARY_PATH'))<5:
          logger.info('Suspicous LD_LIBRARY_PATH: %s'%os.getenv('LD_LIBRARY_PATH'))
  
  #-- figure out paths
  home = os.getenv('HOME')
  genii = home+'/genii'
  cache = {}
  cache['idx'] = '-c'
  cache['col'] = '-c'

#-- allow us to specify a write engine branch
  opts, args = getopt.getopt(sys.argv[1:], 'w:n:u', ['wedir=', 'nocache=', 'usage'])
  wedir = genii+'/writeengine'
  for opt, arg in opts:
      if opt =='-w' or opt =='--wedir':
          wedir = arg
      
      if opt == '-n' or opt == '--nocache':
          if (arg=='idx' or arg=='col'):
              cache[arg] = ''
              logger.info("No cache for %s"% arg)
      
      if opt == '-u' or opt == '--usage':
          usage()
          sys.exit()
          
  logger.info("Using writengine at %s"%wedir)
  
  (bulkroot, dbroot) = find_paths()

  logger.info ("Bulkroot: %s \tDBRoot: %s\n"%(bulkroot, dbroot))

  check_dirs(bulkroot, dbroot)
  
  if len(glob.glob(bulkroot+'/data/import/*tbl')) == 0: 
    sys.exit("No files for import found in BulkRoot: %s"%(bulkroot)) 
  
  if  len(glob.glob(dbroot+'/000.dir'))==0:
    logger.info("No files found in DBRoot: %s (not fatal)"%dbroot)

## force rebuild cpimport and build ipcs-pat if required
  
  build_tool({'path':genii+'/versioning/BRM/',
              'tool':'ipcs-pat',
              'builder':'make', 'args':'tools'})

  build_tool({'path':wedir+'/bulk/',
            'tool':'cpimport',
            'builder':'make', 'args':'clean'})
  try:
    exec_cmd('rm -f', wedir+'/bulk/cpimport')
  except:
      pass
  
  try:
    os.lstat(start_dir+'/cpimport') # look in local directory first
  except:
    build_tool({'path':wedir+'/bulk/',
              'tool':'cpimport',
              'builder':'make', 'args':'cpimport'})
  

## clean up before starting
## remove old db files, removed old temp files, remove shared memory segments, 
## kill old PrimProc and start new one

  logger.info ("Removing old DB files")
  exec_cmd('rm -fr ', dbroot+'/000.dir')

  logger.info ("Removing old temp files")
  exec_cmd('rm -fr ', bulkroot+'/data/import/*.idx.txt')

  logger.info ("Removing old process files")
  exec_cmd('rm -fr ', bulkroot+'/process/*.*')

  logger.info("Killing primProc")
  os.system('killall -q -u $USER PrimProc')

  logger.info ("kill controllernode and workernode")
  exec_cmd(genii+'/export/bin/dbrm', "stop ")

  time.sleep(2);
  logger.info ("Removing shared memory segments")
  exec_cmd(genii+'/versioning/BRM/ipcs-pat', '-d')

  logger.info("Starting controllernode workernode")
  exec_cmd(genii+'/export/bin/dbrm', "start ")

  logger.info("Starting primProc")
  exec_cmd(genii+'/export/bin/PrimProc', "> primproc.log &")

## run dbbuilder - add yes command at front to automatically answer questions
  logger.info ("Building db and indexes (no data inserted)")
  exec_cmd('yes | '+genii+'/tools/dbbuilder/dbbuilder', ' 5')

  logger.info ("Relocating OID files")

  for xmlfile in glob.glob('./Job*xml'):
    logger.info ("Copying %s to %s\n"%(xmlfile,  bulkroot+'/job'))
    # use os.rename instead of shutil.move to avoid problems traversing devices 
    os.rename(xmlfile, bulkroot+'/job/'+xmlfile)

  logger.info("Using cpimport at %s"%(wedir+'/bulk/cpimport'))
  exec_cmd('time '+wedir+'/bulk/cpimport', '-j 299 ')
  exec_cmd(wedir+'/bulk/cpimport', '-c -j 300  ' )

## the following line allows either interactive use or module import
if __name__=="__main__": main()
