#!/usr/bin/env python

"""batch_registration.py:  This script uses the pbs_python library to submit
   Fiji alignment jobs for each tiff stack in a directory"""

import fnmatch
import getopt
import os
import pbs
import sys
import textwrap
from tempfile import mkstemp
import string
import socket


VERSION = "0.1.0"
DEFAULT_WALLTIME = "4:00:00"
DEFAULT_MEM = 14
DEFAULT_VMEM = 25
FIJI_HOME = "/opt/compsci/Fiji/Fiji.app"
MACRO = "/opt/compsci/Fiji/macros/kidneyalign.ijm"


"""
SubmitScript is a class that can generate a batch submission script from a 
template.

"""
class SubmitScript(object):
    
    #the template script, which will be customized for each job
    script_template = textwrap.dedent("""\
        #!/bin/bash
        
        module load Python
        module load java
        
        cd $$PBS_O_WORKDIR
        
        $FIJI_HOME_DIR/ImageJ-linux64  $HEAP -batch $ALIGN_MACRO "$STACK_IN:$STACK_OUT"
    
    """)
        
    def __init__(self, input, output=None, heap=None):
        # tokens will contain tokens that we will substitute when we genrate an instance of the script
        self._tokens = {}
    
        
        # set all of the tokens, which will be used to create an instance of
        # the script later
        self._tokens['STACK_IN'] = input
        
        if output:
            self._tokens['STACK_OUT'] = output
        else:
            filename,extension = os.path.splitext(input)
            self._tokens['STACK_OUT'] = "{0}-aligned{1}".format(os.path.basename(filename),extension)
            
        self._tokens['FIJI_HOME_DIR'] = FIJI_HOME
        self._tokens['ALIGN_MACRO'] = MACRO
        
        if heap:
            self._tokens['HEAP'] = "--heap {0}".format(heap)
        else:
            self._tokens['HEAP'] = ""
    
    """
        use ._tokens to perform keywork subsitution in script template and return
        an instance of the script as a string
    """
    def generate_script(self):
        for key, val in self._tokens.iteritems():
            if val is None:
                raise Exception("Can not perform keyword subsitution in script template: uninitialized token %s" % key)
        return string.Template(self.script_template).substitute(self._tokens)

        
"""
    BatchJob encapsulates a TORQUE job. The submit method creates the TORQUE
    attropl struct and populates it before calling pbs_connect, pbs_submit, and
    pbs_disconnect

"""
class BatchJob(object):

    def __init__(self, job_script, name=None, walltime=None, nodes=None, 
                 stderr=None, stdout=None, workdir=None, mail_options=None, 
                 dependency_list=None, mem=None, vmem=None):
                 
        # these are "private" member variables used by getters/setters
        # we have defined a setter for these so that we can keep a count of 
        # how many of the optional job attributes are set
        self._name = None
        self._walltime = None
        self._nodes = None
        self._stderr_path = None
        self._stdout_path = None
        self._workdir = None
        self._dependency_list = None
        self._mail_options = None
        self._mem = None
        self._vmem = None
        
        self.attribute_count = 0
        self.name = name
        self.job_script = job_script
        self.walltime = walltime
        self.nodes = nodes
        self.stdout_path = stdout
        self.stderr_path = stderr
        self.workdir = workdir
        self.job_id = None
        self.mail_options = mail_options
        self.dependency_list = dependency_list
        self.mem = mem
        self.vmem = vmem
        


    def set_name(self, name):
        if not self.name and name:
            self.attribute_count += 1
        elif name is None and self.name:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._name = name

    def get_name(self):
       return self._name

    name = property(get_name, set_name)

       
    def set_walltime(self, walltime):
        if not self.walltime and walltime:
            self.attribute_count += 1
        elif walltime is None and self.walltime:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._walltime = walltime

    def get_walltime(self):
       return self._walltime
       
    walltime = property(get_walltime, set_walltime)
       
       
    def set_nodes(self, nodes):
        if not self.nodes and nodes:
            self.attribute_count += 1
        elif nodes is None and self.nodes:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._nodes = nodes

    def get_nodes(self):
       return self._nodes
       
    nodes = property(get_nodes, set_nodes)
       
       
    def set_stderr_path(self, stderr_path):
        if not self.stderr_path and stderr_path:
            self.attribute_count += 1
        elif stderr_path is None and self.stderr_path:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._stderr_path = stderr_path

    def get_stderr_path(self):
       return self._stderr_path
       
    stderr_path = property(get_stderr_path, set_stderr_path)
       
       
    def set_stdout_path(self, stdout_path):
        if not self.stdout_path and stdout_path:
            self.attribute_count += 1
        elif stdout_path is None and self.stdout_path:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._stdout_path = stdout_path

    def get_stdout_path(self):
       return self._stdout_path
       
    stdout_path = property(get_stdout_path, set_stdout_path)
    
    
    def set_dependency_list(self, dependency_list):
        if not self.dependency_list and dependency_list:
            self.attribute_count += 1
        elif dependency_list is None and self.dependency_list:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._dependency_list = dependency_list
        
    def get_dependency_list(self):
        return self._dependency_list
        
    dependency_list = property(get_dependency_list, set_dependency_list)
    
    
    def set_workdir(self, workdir):
        if not workdir:
            # if a working directory was not specified default to the current 
            # working directory
            self._workdir = os.getcwd()
        else:
            self._workdir = workdir
            
    def get_workdir(self):
        return self._workdir
        
    workdir = property(get_workdir, set_workdir)
   
    
    def set_mail_options(self, mail_options):
        if not self.mail_options and mail_options:
            self.attribute_count += 1
        elif mail_options is None and self.mail_options:
            self.attribute_count -= 1
        self._mail_options = mail_options
        
    def get_mail_options(self):
        return self._mail_options
        
    mail_options = property(get_mail_options, set_mail_options)
    

    def set_mem(self, mem):
        if not self.mem and mem:
            self.attribute_count += 1
        elif mem is None and self.mem:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._mem = mem
        
    def get_mem(self):
        return self._mem
        
    mem = property(get_mem, set_mem)
    
    
    def set_vmem(self, vmem):
        if not self.vmem and vmem:
            self.attribute_count += 1
        elif vmem is None and self.vmem:
            # attribute was set
            # we are setting it to None now so we want to decriment attribute_count
            self.attribute_count -= 1
        self._vmem = vmem        

    def get_vmem(self):
        return self._vmem
        
    vmem = property(get_vmem, set_vmem)


    """
    submit - submit the job to the batch system
    """
    def submit(self):
 
 
        attropl = pbs.new_attropl(self.attribute_count + 1)
        attropl_idx = 0
 
        attropl[attropl_idx].name  = pbs.ATTR_v
        attropl[attropl_idx].value = self.generate_env()
        attropl_idx += 1
 
        if self.name:
            attropl[attropl_idx].name   = pbs.ATTR_N
            attropl[attropl_idx].value  = self.name
            attropl_idx += 1
           
        if self.walltime:
            attropl[attropl_idx].name     = pbs.ATTR_l
            attropl[attropl_idx].resource = 'walltime'
            attropl[attropl_idx].value    = self.walltime
            attropl_idx += 1
        
        if self.nodes:
            attropl[attropl_idx].name     = pbs.ATTR_l
            attropl[attropl_idx].resource = 'nodes'
            attropl[attropl_idx].value    = self.nodes
            attropl_idx += 1
           
        if self.stdout_path:
            attropl[attropl_idx].name  = pbs.ATTR_o
            attropl[attropl_idx].value = self.stdout_path
            attropl_idx += 1

        if self.stderr_path:
            attropl[attropl_idx].name  = pbs.ATTR_o
            attropl[attropl_idx].value = self.stderr_path
            attropl_idx += 1
           
        if self.dependency_list:
            attropl[attropl_idx].name = pbs.ATTR_depend
            attropl[attropl_idx].value = self.dependency_list
            attropl_idx += 1
           
        if self.mail_options:
            attropl[attropl_idx].name = pbs.ATTR_m
            attropl[attropl_idx].value = self.mail_options
            attropl_idx += 1
           
        if self.mem:
            attropl[attropl_idx].name     = pbs.ATTR_l
            attropl[attropl_idx].resource = 'mem'
            attropl[attropl_idx].value    = self.mem
            attropl_idx += 1
            
        if self.vmem:
            attropl[attropl_idx].name     = pbs.ATTR_l
            attropl[attropl_idx].resource = 'vmem'
            attropl[attropl_idx].value    = self.vmem
            attropl_idx += 1
            
        connection = pbs.pbs_connect(pbs.pbs_default())
        
        self.job_id = pbs.pbs_submit(connection, attropl, self.job_script, None, None)
       
        pbs.pbs_disconnect(connection)
        
        e, e_msg = pbs.error()
        
        # the batch system returned an error, throw exception 
        if e:
            message = "%d: %s" % (e, e_msg)
            raise Exception(message)
            
        return self.job_id

       
    """
    generate_env - generate a basic environment string to send along with the 
    job. This can define any environment variables we want defined in the job's
    environment when it executes. We define some of the typical PBS_O_* variables
    """
    def generate_env(self):
    
        # most of our scripts start with "cd $PBS_O_WORKDIR", so make sure we set it
        env = "PBS_O_WORKDIR=%s" % self.workdir
        
        # define some of the other typical PBS_O_* environment variables
        # PBS_O_HOST is used to set default stdout/stderr paths, the rest probably
        # aren't necessary
        
        env = "".join([env, ",PBS_O_HOST=", socket.getfqdn()])
        if os.environ['PATH']:
            env = "".join([env, ",PBS_O_PATH=", os.environ['PATH']])
        if os.environ['HOME']:
            env = "".join([env, ",PBS_O_HOME=", os.environ['HOME']])
        if os.environ['LOGNAME']:
            env = "".join([env, ",PBS_O_LOGNAME=", os.environ['LOGNAME']])
        
        return env


def usage():

    print "\nUSAGE INFORMATION FOR %s Version %s\n" % (os.path.basename(sys.argv[0]), VERSION)
    
    print "Synopsis: \n", os.path.basename(sys.argv[0]), "[-w, --walltime HH:MM:SS] [-m, --mem <mem in gb>] \\\n" \
          "  [-v, --vmem <vmem in gb>]  [-h, --help] <tiff stack directory>\n"
          
    print "Description:\n" \
           "This script takes a directory of tiff files as input. For each tiff file\n" \
           "in the directory, it submits a batch job that aligns the stack\n" \
           "using a Fiji macro.\n\n"
           
    print "Options:\n" \
          "\t-h, --help\n" \
          "\t\tPrint this helpful message and exit\n\n" \
          "\t-w, --walltime HH:MM:SS\n" \
          "\t\tSpecify a walltime for the Fiji jobs (Default: {0})\n\n" \
          "\t-m, --mem <memory_limit>\n" \
          "\t\tSpecify a physical memory limit for each batch job\n" \
          "\t\tin gigabyes. (Default: {1})\n\n" \
          "\t-v, --vmem <virtual_memory_limit>\n" \
          "\t\tSpecify a virtual memory limit for each batch job\n" \
          "\t\tin gigabyes. (Default: {2})\n\n" \
          "\n".format(DEFAULT_WALLTIME, DEFAULT_MEM, DEFAULT_VMEM)


def main():

    input_directory = None
    walltime=DEFAULT_WALLTIME
    mem = DEFAULT_MEM
    vmem = DEFAULT_VMEM
    
    # parse command line options
    try:
        options, remainder = getopt.gnu_getopt(sys.argv[1:], 'w:m:v:h', ['walltime=', 'mem=', 'vmem=', 'help'])
        
    except getopt.GetoptError, err:
        print str(err)
        usage()
        return 2
        
    for opt, arg in options:
        if opt in ('h', '--help'):
            usage()
            return 0
        if opt in ('-w', '--walltime'):
            walltime = arg
        if opt in ('-m', '--mem'):
            mem = int(arg)
        if opt in ('-v', '--vmem'):
            vmem = int(arg)
        
    if len(remainder) != 1:
        print >> sys.stderr, "\nError: missing required argument!"
        usage()
        return 1

    stack_dir = remainder[0]

    # set the max heap to slightly less than the total amount of physical memory
    # allocated to the job

    heap = "{0}".format(int(mem * 1024 * .93))
    print "setting heap to {0}".format(heap)
    
    mem = "{0}gb".format(mem)
    vmem = "{0}gb".format(vmem)
    
    for file in os.listdir(stack_dir):
        filename,extension = os.path.splitext(file)

        input_file = os.path.join(stack_dir, file)
        if extension.upper() == ".TIFF" or extension.upper() == ".TIF":
            print "Creating job to process {0}".format(input_file)
            
            batch_script = SubmitScript(input_file, heap=heap)
            fd, tmp_filename = mkstemp(suffix=".sh")

            os.write(fd, batch_script.generate_script())
            os.close(fd)
            
            batch_job = BatchJob(job_script=tmp_filename, walltime=walltime, 
                                 name="align_{0}".format(file), 
                                 nodes="1:ppn=8", mail_options='ae', mem=mem, vmem=vmem)
            
            id = batch_job.submit()
            print "\tSubmitted with batch id {0}".format(id)
            os.remove(tmp_filename)
            

if __name__ == '__main__': 
    main() 
        
