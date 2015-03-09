#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import logging
import os
import pipes
import random
import shutil
import subprocess
import sys
import tempfile
import time
import urllib2
import string
from optparse import OptionParser
from sys import stderr
from string import Template
import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, EBSBlockDeviceType
from boto import ec2
import datetime
from datetime import datetime
from datetime import timedelta

class UsageError(Exception):
  pass

DEFAULT_SPARK_VERSION = "1.2.0"
SPARK_EC2_DIR = os.path.dirname(os.path.realpath(__file__))
MESOS_SPARK_EC2_BRANCH = "v4"
# A URL prefix from which to fetch AMI information
AMI_PREFIX = "https://raw.github.com/mesos/spark-ec2/{b}/ami-list".format(b=MESOS_SPARK_EC2_BRANCH)


# Configure and parse our command-line arguments
def parse_args():
  parser = OptionParser(usage="spark-ec2 [options] <action> <cluster_name>"
      + "\n\n<action> can be: launch, destroy, login, stop, start, get-master",
      add_help_option=False)
  parser.add_option("-h", "--help", action="help",
                    help="Show this help message and exit")
  parser.add_option("-s", "--slaves", type="int", default=1,
      help="Number of slaves to launch (default: 1)")
  parser.add_option("-w", "--wait", type="int", default=120,
      help="Seconds to wait for nodes to start (default: 120)")
  parser.add_option("-k", "--key-pair",
      help="Key pair to use on instances")
  parser.add_option("-i", "--identity-file",
      help="SSH private key file to use for logging into instances")
  parser.add_option("-t", "--instance-type", default="m1.large",
      help="Type of instance to launch (default: m1.large). " +
           "WARNING: must be 64-bit; small instances won't work")
  parser.add_option("-m", "--master-instance-type", default="",
      help="Master instance type (leave empty for same as instance-type)")
  parser.add_option("-r", "--region", help="EC2 region zone to launch instances in")
  parser.add_option("-z", "--zone", help="Availability zone to launch instances in, or 'all' to spread " +
           "slaves across multiple (an additional $0.01/Gb for bandwidth" +
           "between zones applies)")
  parser.add_option("-a", "--ami", help="Amazon Machine Image ID to use")
  parser.add_option("-p", "--profile", help="AWS profile/role arn to use")
  parser.add_option("-v", "--spark-version", default=DEFAULT_SPARK_VERSION,
      help="Version of Spark to use: 'X.Y.Z' or a specific git hash")
  parser.add_option("--spark-git-repo",
      default="https://github.com/apache/spark",
      help="Github repo from which to checkout supplied commit hash")
  parser.add_option("--hadoop-major-version", default="2",
      help="Major version of Hadoop (default: 2)")
  parser.add_option("-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
      help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
            "the given local address (for use with login)")
  parser.add_option("--resume", action="store_true", default=False,
      help="Resume installation on a previously launched cluster " +
           "(for debugging)")
  parser.add_option("--ebs-vol-size", metavar="SIZE", type="int", default=0,
      help="Attach a new EBS volume of size SIZE (in GB) to each node as " +
           "/vol. The volumes will be deleted when the instances terminate. " +
           "Only possible on EBS-backed AMIs.")
  parser.add_option("--swap", metavar="SWAP", type="int", default=1024,
      help="Swap space to set up per node, in MB (default: 1024)")
  parser.add_option("--spot-price", metavar="PRICE", type="float",
      help="If specified, launch slaves as spot instances with the given " +
            "maximum price (in dollars)")
  parser.add_option("--ganglia", action="store_true", default=True,
      help="Setup Ganglia monitoring on cluster (default: on). NOTE: " +
           "the Ganglia page will be publicly accessible")
  parser.add_option("--no-ganglia", action="store_false", dest="ganglia",
      help="Disable Ganglia monitoring for the cluster")
  parser.add_option("-u", "--user", default="root",
      help="The SSH user you want to connect as (default: root)")
  parser.add_option("--delete-groups", action="store_true", default=False,
      help="When destroying a cluster, delete the security groups that were created")
  parser.add_option("--use-existing-master", action="store_true", default=False,
      help="Launch fresh slaves, but use an existing stopped master if possible")
  parser.add_option("--worker-instances", type="int", default=1,
      help="Number of instances per worker: variable SPARK_WORKER_INSTANCES (default: 1)")
  parser.add_option("--master-opts", type="string", default="",
      help="Extra options to give to master through SPARK_MASTER_OPTS variable (e.g -Dspark.worker.timeout=180)")

  (opts, args) = parser.parse_args()
  if len(args) != 2:
    parser.print_help()
    sys.exit(1)
  (action, cluster_name) = args

  if opts.region is None:
    opts.region = region()

  if opts.zone is None:
    opts.zone = zone()

  # Boto config check
  # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
  # home_dir = os.getenv('HOME')
  # if home_dir == None or not os.path.isfile(home_dir + '/.boto'):
  #   if not os.path.isfile('/etc/boto.cfg'):
  #     if os.getenv('AWS_ACCESS_KEY_ID') == None:
  #       print >> stderr, ("ERROR: The environment variable AWS_ACCESS_KEY_ID " +
  #                         "must be set")
  #       sys.exit(1)
  #     if os.getenv('AWS_SECRET_ACCESS_KEY') == None:
  #       print >> stderr, ("ERROR: The environment variable AWS_SECRET_ACCESS_KEY " +
  #                         "must be set")
  #       sys.exit(1)
  return (opts, action, cluster_name)


# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name):
  groups = conn.get_all_security_groups()
  group = [g for g in groups if g.name == name]
  if len(group) > 0:
    return group[0]
  else:
    print "Creating security group " + name
    return conn.create_security_group(name, "Spark EC2 group")


# Wait for a set of launched instances to exit the "pending" state
# (i.e. either to start running or to fail and be terminated)
def wait_for_instances(conn, instances):
  ids = [i.id for i in instances]
  while True:
    # for i in instances:
    #   i.update()
      # if len([i for i in instances if i.state == 'pending']) > 0:
  #
    instace_stati = conn.get_all_instance_status(instance_ids=ids)
    if len([i for i in instace_stati if i.system_status.details['reachability'] != 'passed' or i.instance_status.details['reachability'] != 'passed']) > 0:
        time.sleep(5)
    else:
      return


# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

# Return correct versions of Spark and Shark, given the supplied Spark version
def get_spark_shark_version(opts):
  spark_shark_map = {
    "0.7.3": "0.7.1",
    "0.8.0": "0.8.0",
    "0.8.1": "0.8.1",
    "0.9.0": "0.9.0",
    "0.9.1": "0.9.1",
    "1.0.0": "1.0.0",
    "1.0.1": "1.0.1",
    "1.0.2": "1.0.2",
    "1.1.0": "1.1.0",
    "1.2.0": "1.2.0"
  }
  version = opts.spark_version.replace("v", "")
  if version not in spark_shark_map:
    print >> stderr, "Don't know about Spark version: %s" % version
    sys.exit(1)
  return (version, spark_shark_map[version])

# Attempt to resolve an appropriate AMI given the architecture and
# region of the request.
# Information regarding Amazon Linux AMI instance type was updated on 2014-6-20:
# http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
def get_spark_ami(opts):
  instance_types = {
    "c1.medium": "pvm",
    "c1.xlarge": "pvm",
    "c3.2xlarge": "pvm",
    "c3.4xlarge": "pvm",
    "c3.8xlarge": "pvm",
    "c3.large": "pvm",
    "c3.xlarge": "pvm",
    "cc1.4xlarge": "hvm",
    "cc2.8xlarge": "hvm",
    "cg1.4xlarge": "hvm",
    "cr1.8xlarge": "hvm",
    "hi1.4xlarge": "pvm",
    "hs1.8xlarge": "pvm",
    "i2.2xlarge": "hvm",
    "i2.4xlarge": "hvm",
    "i2.8xlarge": "hvm",
    "i2.xlarge": "hvm",
    "m1.large": "pvm",
    "m1.medium": "pvm",
    "m1.small": "pvm",
    "m1.xlarge": "pvm",
    "m2.2xlarge": "pvm",
    "m2.4xlarge": "pvm",
    "m2.xlarge": "pvm",
    "m3.2xlarge": "hvm",
    "m3.large": "hvm",
    "m3.medium": "hvm",
    "m3.xlarge": "hvm",
    "r3.2xlarge": "hvm",
    "r3.4xlarge": "hvm",
    "r3.8xlarge": "hvm",
    "r3.large": "hvm",
    "r3.xlarge": "hvm",
    "t1.micro": "pvm",
    "t2.medium": "hvm",
    "t2.micro": "hvm",
    "t2.small": "hvm",
  }

  if opts.instance_type in instance_types:
    instance_type = instance_types[opts.instance_type]
  else:
    instance_type = "pvm"
    print >> stderr,\
        "Don't recognize %s, assuming type is pvm" % opts.instance_type

  ami_path = "%s/%s/%s" % (AMI_PREFIX, opts.region, instance_type)
  try:
    ami = urllib2.urlopen(ami_path).read().strip()
    print "Spark AMI: " + ami
  except:
    print >> stderr, "Could not resolve AMI at: " + ami_path
    sys.exit(1)

  return ami

# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the master and slaves
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):

  #Remove known hosts to avoid "Offending key for IP ..." errors.
  known_hosts = os.environ['HOME'] + "/.ssh/known_hosts"
  if os.path.isfile(known_hosts):
    os.remove(known_hosts)
  if opts.key_pair is None:
      opts.key_pair = keypair()
      if opts.key_pair is None:
        print >> stderr, "ERROR: Must provide a key pair name (-k) to use on instances."
        sys.exit(1)

  if opts.profile is None:
    opts.profile = profile()
    if opts.profile is None:
      print >> stderr, "ERROR: No profile found in current host. It be provided with -p option."
      sys.exit(1)

  public_key = pub_key()
  user_data = Template("""#!/bin/bash
  set -e -x
  echo '$public_key' >> ~root/.ssh/authorized_keys
  echo '$public_key' >> ~ec2-user/.ssh/authorized_keys""").substitute(public_key=public_key)

  print "Setting up security groups..."
  master_group = get_or_make_group(conn, cluster_name + "-master")
  slave_group = get_or_make_group(conn, cluster_name + "-slaves")
  sparknotebook_group = get_or_make_group(conn, "SparkNotebookApplication")
  if master_group.rules == []: # Group was just now created
    master_group.authorize(src_group=master_group)
    master_group.authorize(src_group=slave_group)
    master_group.authorize(src_group=sparknotebook_group)
    master_group.authorize('tcp', 22, 22, '0.0.0.0/0')
    master_group.authorize('tcp', 8080, 8081, '0.0.0.0/0')
    master_group.authorize('tcp', 18080, 18080, '0.0.0.0/0')
    master_group.authorize('tcp', 19999, 19999, '0.0.0.0/0')
    master_group.authorize('tcp', 50030, 50030, '0.0.0.0/0')
    master_group.authorize('tcp', 50070, 50070, '0.0.0.0/0')
    master_group.authorize('tcp', 60070, 60070, '0.0.0.0/0')
    master_group.authorize('tcp', 4040, 4045, '0.0.0.0/0')
    master_group.authorize('tcp', 7077, 7077, '0.0.0.0/0')
    if opts.ganglia:
      master_group.authorize('tcp', 5080, 5080, '0.0.0.0/0')
  if slave_group.rules == []: # Group was just now created
    slave_group.authorize(src_group=master_group)
    slave_group.authorize(src_group=slave_group)
    slave_group.authorize(src_group=sparknotebook_group)
    slave_group.authorize('tcp', 22, 22, '0.0.0.0/0')
    slave_group.authorize('tcp', 8080, 8081, '0.0.0.0/0')
    slave_group.authorize('tcp', 50060, 50060, '0.0.0.0/0')
    slave_group.authorize('tcp', 50075, 50075, '0.0.0.0/0')
    slave_group.authorize('tcp', 60060, 60060, '0.0.0.0/0')
    slave_group.authorize('tcp', 60075, 60075, '0.0.0.0/0')

  if not any(r for r in sparknotebook_group.rules for g in r.grants if master_group.id == g.group_id):
    sparknotebook_group.authorize(ip_protocol="tcp", from_port="1", to_port="65535", src_group=master_group)
    sparknotebook_group.authorize(ip_protocol="icmp", from_port="-1", to_port="-1", src_group=master_group)

  if not any(r for r in sparknotebook_group.rules for g in r.grants if slave_group.id == g.group_id):
    sparknotebook_group.authorize(ip_protocol="tcp", from_port="1", to_port="65535", src_group=slave_group)
    sparknotebook_group.authorize(ip_protocol="icmp", from_port="-1", to_port="-1", src_group=slave_group)

  # Check if instances are already running in our groups
  existing_masters, existing_slaves = get_existing_cluster(conn, opts, cluster_name,
                                                           die_on_error=False)
  if existing_slaves or (existing_masters and not opts.use_existing_master):
    print >> stderr,("ERROR: There are already instances running in " +
        "group %s or %s" % (master_group.name, slave_group.name))
    sys.exit(1)

  # Figure out Spark AMI
  if opts.ami is None:
    opts.ami = get_spark_ami(opts)
  print "Launching instances..."

  try:
    image = conn.get_all_images(image_ids=[opts.ami])[0]
  except:
    print >> stderr,"Could not find AMI " + opts.ami
    sys.exit(1)

  # Create block device mapping so that we can add an EBS volume if asked to
  block_map = BlockDeviceMapping()
  if opts.ebs_vol_size > 0:
    device = EBSBlockDeviceType()
    device.size = opts.ebs_vol_size
    device.delete_on_termination = True
    block_map["/dev/sdv"] = device

  # Launch slaves
  if opts.spot_price != None:
    zones = get_zones(conn, opts)
    
    num_zones = len(zones)
    i = 0
    my_req_ids = []

    for zone in zones:
      best_price = find_best_price(conn,opts.instance_type,zone, opts.spot_price)
      # Launch spot instances with the requested price
      print >> stderr,("Requesting %d slaves as spot instances with price $%.3f/hour each (total $%.3f/hour)" %
           (opts.slaves, best_price, opts.slaves * best_price))

      num_slaves_this_zone = get_partition(opts.slaves, num_zones, i)
      interface = boto.ec2.networkinterface.NetworkInterfaceSpecification(subnet_id=subnetId(), groups=[slave_group.id], associate_public_ip_address=True)
      interfaces = boto.ec2.networkinterface.NetworkInterfaceCollection(interface)

      slave_reqs = conn.request_spot_instances(
          price = best_price,
          image_id = opts.ami,
          launch_group = "launch-group-%s" % cluster_name,
          placement = zone,
          count = num_slaves_this_zone,
          key_name = opts.key_pair,
          instance_type = opts.instance_type,
          block_device_map = block_map,
          user_data = user_data,
          instance_profile_arn = opts.profile,
          network_interfaces = interfaces)
      my_req_ids += [req.id for req in slave_reqs]
      i += 1

    print >> stderr, "Waiting for spot instances to be granted"
    try:
      while True:
        time.sleep(10)
        reqs = conn.get_all_spot_instance_requests()
        id_to_req = {}
        for r in reqs:
          id_to_req[r.id] = r
        active_instance_ids = []
        for i in my_req_ids:
          if i in id_to_req and id_to_req[i].state == "active":
            active_instance_ids.append(id_to_req[i].instance_id)
        if len(active_instance_ids) == opts.slaves:
          print >> stderr, "All %d slaves granted" % opts.slaves
          reservations = conn.get_all_instances(active_instance_ids)
          slave_nodes = []
          for r in reservations:
            slave_nodes += r.instances
          break
        else:
          # print >> stderr, ".",
          print "%d of %d slaves granted, waiting longer" % (
            len(active_instance_ids), opts.slaves)
    except:
      print >> stderr, "Canceling spot instance requests"
      conn.cancel_spot_instance_requests(my_req_ids)
      # Log a warning if any of these requests actually launched instances:
      (master_nodes, slave_nodes) = get_existing_cluster(
          conn, opts, cluster_name, die_on_error=False)
      running = len(master_nodes) + len(slave_nodes)
      if running:
        print >> stderr,("WARNING: %d instances are still running" % running)
      sys.exit(0)
  else:
    # Launch non-spot instances
    zones = get_zones(conn, opts)
    num_zones = len(zones)
    i = 0
    slave_nodes = []
    for zone in zones:
      num_slaves_this_zone = get_partition(opts.slaves, num_zones, i)
      if num_slaves_this_zone > 0:
        slave_res = image.run(key_name = opts.key_pair,
                              security_group_ids = [slave_group.id],
                              instance_type = opts.instance_type,
                              subnet_id = subnetId(),
                              placement = zone,
                              min_count = num_slaves_this_zone,
                              max_count = num_slaves_this_zone,
                              block_device_map = block_map,
                              user_data = user_data,
                              instance_profile_arn = opts.profile)
        slave_nodes += slave_res.instances
        print >> stderr,"Launched %d slaves in %s, regid = %s" % (num_slaves_this_zone,
                                                        zone, slave_res.id)
      i += 1

  # Launch or resume masters
  if existing_masters:
    print "Starting master..."
    for inst in existing_masters:
      if inst.state not in ["shutting-down", "terminated"]:
        inst.start()
    master_nodes = existing_masters
  else:
    master_type = opts.master_instance_type
    if master_type == "":
      master_type = opts.instance_type
    if opts.zone == 'all':
      opts.zone = random.choice(conn.get_all_zones()).name
    if opts.spot_price != None:
      best_price = find_best_price(conn,master_type,opts.zone,opts.spot_price)
      # Launch spot instances with the requested price
      print >> stderr, ("Requesting master as spot instances with price $%.3f/hour" % (best_price))

      interface = boto.ec2.networkinterface.NetworkInterfaceSpecification(subnet_id=subnetId(), groups=[master_group.id], associate_public_ip_address=True)
      interfaces = boto.ec2.networkinterface.NetworkInterfaceCollection(interface)

      master_reqs = conn.request_spot_instances(
        price = best_price,
        image_id = opts.ami,
        launch_group = "launch-group-%s" % cluster_name,
        placement = opts.zone,
        count = 1,
        key_name = opts.key_pair,
        instance_type = master_type,
        block_device_map = block_map,
        user_data = user_data,
        instance_profile_arn = opts.profile,
        network_interfaces = interfaces)
      my_req_ids = [r.id for r in master_reqs]
      print >> stderr, "Waiting for spot instance to be granted"
      try:
        while True:
          time.sleep(10)
          reqs = conn.get_all_spot_instance_requests(request_ids=my_req_ids)
          id_to_req = {}
          for r in reqs:
            id_to_req[r.id] = r
          active_instance_ids = []
          for i in my_req_ids:
            if i in id_to_req and id_to_req[i].state == "active":
              active_instance_ids.append(id_to_req[i].instance_id)
          if len(active_instance_ids) == 1:
            print >> stderr, "Master granted"
            reservations = conn.get_all_instances(active_instance_ids)
            master_nodes = []
            for r in reservations:
              master_nodes += r.instances
            break
          else:
            # print >> stderr, ".",
            print "%d of %d masters granted, waiting longer" % (
              len(active_instance_ids), 1)
      except:
        print >> stderr, "Canceling spot instance requests"
        conn.cancel_spot_instance_requests(my_req_ids)
        # Log a warning if any of these requests actually launched instances:
        (master_nodes, master_nodes) = get_existing_cluster(
            conn, opts, cluster_name, die_on_error=False)
        running = len(master_nodes) + len(master_nodes)
        if running:
          print >> stderr, ("WARNING: %d instances are still running" % running)
        sys.exit(0)
    else:
      master_res = image.run(key_name = opts.key_pair,
                             security_group_ids = [master_group.id],
                             instance_type = master_type,
                             subnet_id = subnetId(),
                             placement = opts.zone,
                             min_count = 1,
                             max_count = 1,
                             block_device_map = block_map,
                             user_data = user_data,
                             instance_profile_arn = opts.profile)
      master_nodes = master_res.instances
      print >> stderr,"Launched master in %s, regid = %s" % (zone, master_res.id)
  # Return all the instances
  return (master_nodes, slave_nodes)


# Get the EC2 instances in an existing cluster if available.
# Returns a tuple of lists of EC2 instance objects for the masters and slaves
def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
  print >> stderr,"Searching for existing cluster %s ..." % cluster_name
  reservations = conn.get_all_instances()
  master_nodes = []
  slave_nodes = []
  for res in reservations:
    active = [i for i in res.instances if is_active(i)]
    for inst in active:
      group_names = [g.name for g in inst.groups]
      if (cluster_name + "-master") in group_names:
        master_nodes.append(inst)
      elif (cluster_name + "-slaves") in group_names:
        slave_nodes.append(inst)
  if any((master_nodes, slave_nodes)):
    print "Spark standalone cluster started at http://%s:8080" % master_nodes[0].public_dns_name
    print "Spark private ip address %s" % master_nodes[0].private_dns_name
    print >> stderr, "Spark standalone cluster started at http://%s:8080" % master_nodes[0].public_dns_name
    print >> stderr,("Found %d master(s), %d slaves" %
           (len(master_nodes), len(slave_nodes)))
    get_master_setup_files(master_nodes[0].public_dns_name, opts)
    if opts.ganglia:
      print >> stderr,"Ganglia started at http://%s:5080/ganglia" % master_nodes[0].public_dns_name
  if master_nodes != [] or not die_on_error:
    return (master_nodes, slave_nodes)
  else:
    if master_nodes == [] and slave_nodes != []:
      print "ERROR: Could not find master in group %s-master" %cluster_name
    else:
      print "ERROR: Could not find any existing cluster"
    sys.exit(1)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, master_nodes, slave_nodes, opts, deploy_ssh_key):

  master_nodes[0].update()
  master = master_nodes[0]
  print  "Spark private ip address %s" % master.private_dns_name
  if deploy_ssh_key:
    print "Generating cluster's SSH key on master..."
    key_setup = """
      [ -f ~/.ssh/id_rsa ] ||
        (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
         cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
    """
    ssh(master.private_dns_name, opts, key_setup)
    dot_ssh_tar = ssh_read(master.private_dns_name, opts, ['tar', 'c', '.ssh'])
    print >> stderr, "Transferring cluster's SSH key to slaves..."
    for slave in slave_nodes:
      slave.update()
      ssh_write(slave.public_dns_name, opts, ['tar', 'x'], dot_ssh_tar)

  modules = ['spark', 'ephemeral-hdfs', 'persistent-hdfs',
             'mapreduce', 'spark-standalone']

  if opts.hadoop_major_version == "1":
    modules = filter(lambda x: x != "mapreduce", modules)

  if opts.ganglia:
    modules.append('ganglia')

  # NOTE: We should clone the repository before running deploy_files to
  # prevent ec2-variables.sh from being overwritten
  ssh(
    host=master.private_dns_name,
    opts=opts,
    command="rm -rf spark-ec2"
    + " && "
    + "git clone https://github.com/paulomagalhaes/spark-ec2.git -b {b}".format(b=MESOS_SPARK_EC2_BRANCH)
  )

  print >> stderr,"Deploying files to master... "
  (path, name) = os.path.split(__file__)
  deploy_files(conn, path+"/deploy.generic", opts, master_nodes, slave_nodes, modules)

  print >> stderr,"Running setup on master... "
  setup_spark_cluster(master, opts)
  get_master_setup_files(master.private_dns_name, opts)
  print >> stderr,"Done!"

def get_master_setup_files(master, opts):
  scp(master, opts, "spark/lib/datanucleus*.jar", "%s/../lib" % SPARK_EC2_DIR)
  scp(master, opts, "spark/conf/*", SPARK_EC2_DIR)

def setup_standalone_cluster(master, slave_nodes, opts):
  slave_ips = '\n'.join([i.public_dns_name for i in slave_nodes])
  ssh(master, opts, "echo \"%s\" > spark/conf/slaves" % (slave_ips))
  ssh(master, opts, "/root/spark/sbin/start-all.sh")

def setup_spark_cluster(master, opts):
  ssh(master.private_dns_name, opts, "chmod u+x spark-ec2/setup.sh")
  ssh(master.private_dns_name, opts, "spark-ec2/setup.sh")
  print "Spark standalone cluster started at http://%s:8080" % master.public_dns_name
  print >> stderr, "Spark standalone cluster started at http://%s:8080" % master.public_dns_name
  if opts.ganglia:
    print >> stderr,"Ganglia started at http://%s:5080/ganglia" % master.public_dns_name



# Wait for a whole cluster (masters, slaves and ZooKeeper) to start up
def wait_for_cluster(conn, wait_secs, master_nodes, slave_nodes):
  print >> stderr,"Waiting for instances to start up..."
  time.sleep(5)
  wait_for_instances(conn, master_nodes)
  wait_for_instances(conn, slave_nodes)


# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance_type):
  # From http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
  # Updated 2014-6-20
  disks_by_instance = {
    "m1.small":    1,
    "m1.medium":   1,
    "m1.large":    2,
    "m1.xlarge":   4,
    "t1.micro":    1,
    "c1.medium":   1,
    "c1.xlarge":   4,
    "m2.xlarge":   1,
    "m2.2xlarge":  1,
    "m2.4xlarge":  2,
    "cc1.4xlarge": 2,
    "cc2.8xlarge": 4,
    "cg1.4xlarge": 2,
    "hs1.8xlarge": 24,
    "cr1.8xlarge": 2,
    "hi1.4xlarge": 2,
    "m3.medium":   1,
    "m3.large":    1,
    "m3.xlarge":   2,
    "m3.2xlarge":  2,
    "i2.xlarge":   1,
    "i2.2xlarge":  2,
    "i2.4xlarge":  4,
    "i2.8xlarge":  8,
    "c3.large":    2,
    "c3.xlarge":   2,
    "c3.2xlarge":  2,
    "c3.4xlarge":  2,
    "c3.8xlarge":  2,
    "r3.large":    1,
    "r3.xlarge":   1,
    "r3.2xlarge":  1,
    "r3.4xlarge":  1,
    "r3.8xlarge":  2,
    "g2.2xlarge":  1,
    "t1.micro":    0
  }
  if instance_type in disks_by_instance:
    return disks_by_instance[instance_type]
  else:
    print >> stderr, ("WARNING: Don't know number of disks on instance type %s; assuming 1"
                      % instance_type)
    return 1


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of masters and slaves). Files are only deployed to
# the first master instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
def deploy_files(conn, root_dir, opts, master_nodes, slave_nodes, modules):
  active_master = master_nodes[0].public_dns_name

  num_disks = get_num_disks(opts.instance_type)
  hdfs_data_dirs = "/mnt/ephemeral-hdfs/data"
  mapred_local_dirs = "/mnt/hadoop/mrlocal"
  spark_local_dirs = "/mnt/spark"
  if num_disks > 1:
    for i in range(2, num_disks + 1):
      hdfs_data_dirs += ",/mnt%d/ephemeral-hdfs/data" % i
      mapred_local_dirs += ",/mnt%d/hadoop/mrlocal" % i
      spark_local_dirs += ",/mnt%d/spark" % i

  cluster_url = "%s:7077" % active_master

  if "." in opts.spark_version:
    # Pre-built spark & shark deploy
    (spark_v, shark_v) = get_spark_shark_version(opts)
  else:
    # Spark-only custom deploy
    spark_v = "%s|%s" % (opts.spark_git_repo, opts.spark_version)
    shark_v = ""
    modules = filter(lambda x: x != "shark", modules)

  template_vars = {
    "master_list": '\n'.join([i.public_dns_name for i in master_nodes]),
    "active_master": active_master,
    "slave_list": '\n'.join([i.public_dns_name for i in slave_nodes]),
    "cluster_url": cluster_url,
    "hdfs_data_dirs": hdfs_data_dirs,
    "mapred_local_dirs": mapred_local_dirs,
    "spark_local_dirs": spark_local_dirs,
    "swap": str(opts.swap),
    "modules": '\n'.join(modules),
    "spark_version": spark_v,
    "shark_version": shark_v,
    "hadoop_major_version": opts.hadoop_major_version,
    "metastore_user": "hive",
    "metastore_passwd": ''.join(random.SystemRandom().choice(string.uppercase + string.digits) for _ in xrange(10)),
    "spark_worker_instances": "%d" % opts.worker_instances,
    "spark_master_opts": opts.master_opts
  }

  # Create a temp directory in which we will place all the files to be
  # deployed after we substitue template parameters in them
  print root_dir
  tmp_dir = tempfile.mkdtemp()
  for path, dirs, files in os.walk(root_dir):
    if path.find(".svn") == -1:
      dest_dir = os.path.join('/', path[len(root_dir):])
      local_dir = tmp_dir + dest_dir
      if not os.path.exists(local_dir):
        os.makedirs(local_dir)
      for filename in files:
        if filename[0] not in '#.~' and filename[-1] != '~':
          dest_file = os.path.join(dest_dir, filename)
          local_file = tmp_dir + dest_file
          with open(os.path.join(path, filename)) as src:
            with open(local_file, "w") as dest:
              text = src.read()
              for key in template_vars:
                text = text.replace("{{" + key + "}}", template_vars[key])
              dest.write(text)
              dest.close()
  # rsync the whole directory over to the master machine
  command = [
      'rsync', '-rv',
      '-e', stringify_command(ssh_command(opts)),
      "%s/" % tmp_dir,
      "%s@%s:/" % (opts.user, active_master)
    ]
  subprocess.check_call(command)
  # Remove the temp directory we created above
  shutil.rmtree(tmp_dir)
  print tmp_dir


def stringify_command(parts):
  if isinstance(parts, str):
    return parts
  else:
    return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
  parts = ['-o', 'StrictHostKeyChecking=no', '-o LogLevel=quiet']
  # parts += ['-i', '~/.ssh/id_rsa']
  return parts


def ssh_command(opts):
  return ['ssh'] + ssh_args(opts)

def scp_command(opts):
  return ['scp'] + ssh_args(opts)

def pub_key():
  key_gen = """[ -f ~/.ssh/id_rsa ] ||
        (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa)
  """
  subprocess.check_call(key_gen, shell=True)
  return subprocess.Popen("cat ~/.ssh/id_rsa.pub", shell=True, stdout=subprocess.PIPE).communicate()[0]

def profile():
  return subprocess.Popen("""curl -s http://169.254.169.254/latest/meta-data/iam/info | grep InstanceProfileArn""", shell=True, stdout=subprocess.PIPE).communicate()[0].split("\"")[3]

def region():
  return subprocess.Popen("""curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | grep region""", shell=True, stdout=subprocess.PIPE).communicate()[0].split("\"")[3]

def zone():
  return subprocess.Popen("""curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | grep availabilityZone""", shell=True, stdout=subprocess.PIPE).communicate()[0].split("\"")[3]

def subnetId():
  mac = subprocess.Popen("""curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/ | grep /""", shell=True, stdout=subprocess.PIPE).communicate()[0].split("/")[0]
  return subprocess.Popen("""curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/""" + mac + """/subnet-id/""", shell=True, stdout=subprocess.PIPE).communicate()[0]

def keypair():
    return subprocess.Popen("""curl -s  http://169.254.169.254/latest/meta-data/public-keys/0/openssh-key""", shell=True, stdout=subprocess.PIPE).communicate()[0].split(" ")[2].strip()

# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
  tries = 0
  while True:
    try:
      #print >> stderr, ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host), stringify_command(command)]
      return subprocess.check_call(
        ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host), stringify_command(command)])
    except subprocess.CalledProcessError as e:
      if (tries > 10):
        print >> stderr,'Failed to SSH to remote host %s after %s retries.' % (host, tries)
        # If this was an ssh failure, provide the user with hints.
        if e.returncode == 255:
          raise UsageError('Failed to SSH to remote host %s.\nPlease check that you have provided the correct --identity-file and --key-pair parameters and try again.' % (host))
        else:
          raise e
      #print >> stderr,"Error executing remote command, retrying after 30 seconds: {0}".format(e)
      time.sleep(30)
      tries = tries + 1

def scp(host, opts, src, target):
  tries = 0
  while True:
    try:
      return subprocess.check_call(
        scp_command(opts) + ['%s@%s:%s' % (opts.user, host,src), target])
    except subprocess.CalledProcessError as e:
      if (tries > 10):
        print >> stderr,"Failed to SCP to remote host {0} after r retries.".format(host)
        # If this was an ssh failure, provide the user with hints.
        if e.returncode == 255:
          raise UsageError("Failed to SCP to remote host {0}.\nPlease check that you have provided the correct --identity-file and --key-pair parameters and try again.".format(host))
        else:
          raise e
      time.sleep(30)
      tries = tries + 1


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
  return _check_output(
      ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)])


def ssh_write(host, opts, command, input):
  tries = 0
  while True:
    proc = subprocess.Popen(
        ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)],
        stdin=subprocess.PIPE, stderr=subprocess.STDOUT)
    proc.stdin.write(input)
    proc.stdin.close()
    status = proc.wait()
    if status == 0:
      break
    elif (tries > 5):
      raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
    else:
      print >> stderr, "Error {0} while executing remote command, retrying after 30 seconds".format(status)
      time.sleep(30)
      tries = tries + 1


# Gets a list of zones to launch instances in
def get_zones(conn, opts):
  if opts.zone == 'all':
    zones = [z.name for z in conn.get_all_zones()]
  else:
    zones = [opts.zone]
  return zones


# Gets the number of items in a partition
def get_partition(total, num_partitions, current_partitions):
  num_slaves_this_zone = total / num_partitions
  if (total % num_partitions) - current_partitions > 0:
    num_slaves_this_zone += 1
  return num_slaves_this_zone


def real_main():
  (opts, action, cluster_name) = parse_args()
  try:
    conn = ec2.connect_to_region(opts.region)
  except Exception as e:
    print >> stderr,(e)
    sys.exit(1)

  # Select an AZ at random if it was not specified.
  if opts.zone == "":
    opts.zone = random.choice(conn.get_all_zones()).name

  if action == "launch":
    if opts.slaves <= 0:
      print >> sys.stderr, "ERROR: You have to start at least 1 slave"
      sys.exit(1)
    if opts.resume:
      (master_nodes, slave_nodes) = get_existing_cluster(
          conn, opts, cluster_name)
    else:
      start_secs = time.time()
      (master_nodes, slave_nodes) = launch_cluster(
          conn, opts, cluster_name)
      wait_for_cluster(conn, opts.wait, master_nodes, slave_nodes)
      print >> stderr, "Provisioning took %.3f minutes" % ((time.time() - start_secs) / 60.0)
      start_secs = time.time()
      setup_cluster(conn, master_nodes, slave_nodes, opts, True)
      print >> stderr,"Setup took %.3f minutes" % ((time.time() - start_secs)/60.0)

  elif action == "destroy":
    (master_nodes, slave_nodes) = get_existing_cluster(
        conn, opts, cluster_name, die_on_error=False)
    print >> stderr,"Terminating master..."
    for inst in master_nodes:
      inst.terminate()
    print >> stderr,"Terminating slaves..."
    for inst in slave_nodes:
      inst.terminate()

    # Delete security groups as well
    if opts.delete_groups:
      print >> stderr,"Deleting security groups (this will take some time)..."
      group_names = [cluster_name + "-master", cluster_name + "-slaves"]

      attempt = 1;
      while attempt <= 3:
        print >> stderr,"Attempt %d" % attempt
        groups = [g for g in conn.get_all_security_groups() if g.name in group_names]
        success = True
        # Delete individual rules in all groups before deleting groups to
        # remove dependencies between them
        for group in groups:
          print >> stderr,"Deleting rules in security group " + group.name
          for rule in group.rules:
            for grant in rule.grants:
                success &= group.revoke(ip_protocol=rule.ip_protocol,
                         from_port=rule.from_port,
                         to_port=rule.to_port,
                         src_group=grant)

        # Sleep for AWS eventual-consistency to catch up, and for instances
        # to terminate
        time.sleep(30)  # Yes, it does have to be this long :-(
        for group in groups:
          try:
            conn.delete_security_group(group.name)
            print >> stderr,"Deleted security group " + group.name
          except boto.exception.EC2ResponseError:
            success = False;
            print >> stderr,"Failed to delete security group " + group.name

        # Unfortunately, group.revoke() returns True even if a rule was not
        # deleted, so this needs to be rerun if something fails
        if success: break;

        attempt += 1

      if not success:
        print >> stderr,"Failed to delete all security groups after 3 tries."
        print >> stderr,"Try re-running in a few minutes."

  elif action == "login":
    (master_nodes, slave_nodes) = get_existing_cluster(
        conn, opts, cluster_name)
    master = master_nodes[0].public_dns_name
    print "Logging into master " + master + "..."
    proxy_opt = []
    if opts.proxy_port != None:
      proxy_opt = ['-D', opts.proxy_port]
    subprocess.check_call(
        ssh_command(opts) + proxy_opt + ['-t', '-t', "%s@%s" % (opts.user, master)])

  elif action == "get-master":
    (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
    print master_nodes[0].public_dns_name

  elif action == "stop":
    response = raw_input("Are you sure you want to stop the cluster " +
        cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
        "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
        "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
        "All data on spot-instance slaves will be lost.\n" +
        "Stop cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (master_nodes, slave_nodes) = get_existing_cluster(
          conn, opts, cluster_name, die_on_error=False)
      print >> stderr,"Stopping master..."
      for inst in master_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          inst.stop()
      print >> stderr,"Stopping slaves..."
      for inst in slave_nodes:
        if inst.state not in ["shutting-down", "terminated"]:
          if inst.spot_instance_request_id:
            inst.terminate()
          else:
            inst.stop()

  elif action == "start":
    (master_nodes, slave_nodes) = get_existing_cluster(conn, opts, cluster_name)
    print >> stderr,"Starting slaves..."
    for inst in slave_nodes:
      if inst.state not in ["shutting-down", "terminated"]:
        inst.start()
    print >> stderr,"Starting master..."
    for inst in master_nodes:
      if inst.state not in ["shutting-down", "terminated"]:
        inst.start()
    wait_for_cluster(conn, opts.wait, master_nodes, slave_nodes)
    setup_cluster(conn, master_nodes, slave_nodes, opts, False)

  else:
    print >> stderr,"Invalid action: %s" % action
    sys.exit(1)

def find_best_price(conn,instance,zone, factor):
  last_hour_zone = get_spot_price(conn,zone,datetime.utcnow()-timedelta(hours=1),instance)
  average_price_last_hour = sum(i.price for i in last_hour_zone)/float(len(last_hour_zone))
  return average_price_last_hour*factor

def get_spot_price(conn,zone,start_date_hour,instance):
    return conn.get_spot_price_history(start_time=start_date_hour.strftime("%Y-%m-%dT%H:%M:%SZ"),end_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),instance_type=instance , product_description="Linux/UNIX",availability_zone=zone)

def main():
  try:
    real_main()
  except UsageError, e:
    print >> stderr,"\nError:\n", e
    sys.exit(1)


if __name__ == "__main__":
  logging.basicConfig()
  main()
