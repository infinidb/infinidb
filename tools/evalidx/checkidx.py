#!/usr/bin/python

import os, sys, glob, shutil, xml.dom.minidom

def find_paths():
  
  """Find DBRoot and BulkRoot."""
  try:
    config_file = os.environ['CALPONT_CONFIG_FILE']
  except KeyError:
    try:
      config_file = '/usr/local/Calpont/etc'
      os.lstat(config_file)
    except:
      sys.exit('No config file available')

      
  xmldoc = xml.dom.minidom.parse(config_file)
  bulk_node = xmldoc.getElementsByTagName('BulkRoot')[0]
  db_node = xmldoc.getElementsByTagName('DBRoot')[0]
  
  bulk_dir = bulk_node.childNodes[0].nodeValue
  data_dir = db_node.childNodes[0].nodeValue

  return (bulk_dir, data_dir)


def validate_indexes(job_file):
  index_files = []
  xmldoc = xml.dom.minidom.parse(job_file)

  for index_node in xmldoc.getElementsByTagName('Index'):
    curTreeOid = index_node.getAttribute('iTreeOid')
    curListOid = index_node.getAttribute('iListOid')
    curMapOid = index_node.getAttribute('mapOid')
    curIdxCmdArg = ' -t ' + curTreeOid + ' -l ' + curListOid + ' -v -c ' + curMapOid + ' -b 4' + ' > idxCol_' + curMapOid+'.out' 
#    print curIdxCmd
#    exec_cmd( genii + '/tools/evalidx/evalidx', curIdxCmd )
    index_files.append( curIdxCmdArg )
      
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
    res = os.system(cmd+' '+args)
    if res:
      sys.exit( res )
             


def main():
  """
  Validate indexes..
  """
  
  if not os.access('.', os.W_OK):
    os.chdir('/tmp')
    print 'Changing to /tmp to have permission to write files'

  if len(os.getenv('LD_LIBRARY_PATH'))<5:
    print 'Suspicous LD_LIBRARY_PATH: %s'%os.getenv('LD_LIBRARY_PATH')
  
  home = os.getenv('HOME')
  genii = home+'/genii'

  (bulkroot, dbroot) = find_paths()

  if len(glob.glob(bulkroot+'/job/Job_300.xml')) == 0: 
    sys.exit("No Job_300.xml exist ") 
  
  indexes = validate_indexes(bulkroot+'/job/Job_300.xml')
  for idxCmdArg in indexes:
    print idxCmdArg
    exec_cmd( genii + '/tools/evalidx/evalidx', idxCmdArg )
  

## the following line allows either interactive use or module import
if __name__=="__main__": main()
