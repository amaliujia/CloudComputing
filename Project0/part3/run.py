import boto.ec2.elb
import os
import requests
from boto.ec2.elb import HealthCheck
import boto.ec2.autoscale
from boto.ec2.autoscale import AutoScalingGroup
from boto.ec2.autoscale import LaunchConfiguration
import time
from boto.ec2.autoscale import ScalingPolicy
import boto.ec2.cloudwatch
from boto.ec2.cloudwatch import MetricAlarm

Load_Generator = "ami-76164d1c"
Load_Generator_Type = "m3.medium"

Data_Center = "ami-f4144f9e"
Data_Center_Type = "m3.medium"

ACCESS_KEY_ID = os.environ('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.environ('SECRET_ACCESS_KEY')

SUBMISSION_PASSWORD = "jEJEH6og330aT5TqlrYAIMFNqDrqflW4"

SECURITY_GROUP_ID = ['sg-091a7270']
SECURITY_GROUP = ['launch-wizard-7']
KEY_NAME = "P0.1LoadGenerator"

AUTO_SCALE_GROUP='SmartGroup'


# parameters
MIN_INSTANCE_SIZE=2
MAX_INSTANCE_SIZE=4

HEALTHCHECK_INTERVAL=20
HEALTHY_THRESHOLD=3
UNHEALTHY_THRESHOLD=5

SCALE_UP_COOLDOWN=120
SCALE_DOWN_COOLDOWN=120

SCALE_UP_THRESHOLD='65'
SCALE_UP_THRESHOLD_PERIOD='180'

SCALE_DOWN_THRESHOLD='25'
SCALE_DOWN_THRESHOLD_PERIOD='240'

LOAD_GERNRATOR_DNS='ec2-52-91-20-101.compute-1.amazonaws.com'

#create load generator
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
reservation = conn.run_instances(Load_Generator, key_name=KEY_NAME, instance_type=Load_Generator_Type, security_groups=SECURITY_GROUP)

load_generator = reservation.instances[0]
stat = load_generator.update()
while stat == "pending":
    time.sleep(20)
    stat = load_generator.update()

if stat == "running":
    load_generator.add_tag("Name", "Load Generator")

time.sleep(80)
print "load generator dns: " + load_generator.public_dns_name
LOAD_GERNRATOR_DNS =load_generator.public_dns_name

http_request = "http://" + load_generator.public_dns_name + "/password"
response = requests.get(http_request, params={"passwd": SUBMISSION_PASSWORD})

time_interval = 0
while "return to the homepage" not in response.text:
    if time_interval > 10:
        exit()
    response = requests.get(http_request, params={"passwd": SUBMISSION_PASSWORD})
    if "return to the homepage" in response.text:
        break
    time_interval += 1
    time.sleep(10)


# Creat Load Balancer
conn = boto.ec2.elb.connect_to_region("us-east-1", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
zones = ['us-east-1a']
ports = [(80, 80, 'http')]
load_balancer = conn.create_load_balancer('SmartELB', zones, ports, security_groups=SECURITY_GROUP_ID)

health_check = HealthCheck(
        interval=HEALTHCHECK_INTERVAL,
        healthy_threshold=HEALTHY_THRESHOLD,
        unhealthy_threshold=UNHEALTHY_THRESHOLD,
        target=('HTTP:80/heartbeat?lg=' + LOAD_GERNRATOR_DNS)
)

load_balancer.configure_health_check(health_check)

time.sleep(20)
print "load balacner dns: " + load_balancer.dns_name

# create auto scaling group
conn = boto.ec2.autoscale.connect_to_region("us-east-1", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)

lc = LaunchConfiguration(name='m3.medium.non_spot', image_id=Data_Center,
                             key_name='worker', instance_type=Data_Center_Type,
                             security_groups=SECURITY_GROUP)

# How about time to cool down
ag = AutoScalingGroup(group_name='SmartGroup', load_balancers=['SmartELB'],
                          availability_zones=['us-east-1a'],
                          launch_config=lc, min_size=MIN_INSTANCE_SIZE, max_size=MAX_INSTANCE_SIZE, health_check_type='ELB',
                          health_check_period='120', connection=conn)

conn.create_auto_scaling_group(ag)


scale_up_policy = ScalingPolicy(
            name='scale_up', adjustment_type='ChangeInCapacity',
            as_name=AUTO_SCALE_GROUP, scaling_adjustment=1, cooldown=SCALE_UP_COOLDOWN)
scale_down_policy = ScalingPolicy(
            name='scale_down', adjustment_type='ChangeInCapacity',
            as_name=AUTO_SCALE_GROUP, scaling_adjustment=-1, cooldown=SCALE_DOWN_COOLDOWN)

conn.create_scaling_policy(scale_up_policy)
conn.create_scaling_policy(scale_down_policy)

scale_up_policy = conn.get_all_policies(
            as_group=AUTO_SCALE_GROUP, policy_names=['scale_up'])[0]
scale_down_policy = conn.get_all_policies(
            as_group=AUTO_SCALE_GROUP, policy_names=['scale_down'])[0]


cloudwatch = boto.ec2.cloudwatch.connect_to_region("us-east-1", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)
alarm_dimensions = {"AutoScalingGroupName": AUTO_SCALE_GROUP}

scale_up_alarm = MetricAlarm(
            name='scale_up_on_cpu', namespace='AWS/EC2',
            metric='CPUUtilization', statistic='Average',
            comparison='>', threshold='65',
            period='180', evaluation_periods=1,
            alarm_actions=[scale_up_policy.policy_arn],
            dimensions=alarm_dimensions)
cloudwatch.create_alarm(scale_up_alarm)

scale_down_alarm = MetricAlarm(
            name='scale_down_on_cpu', namespace='AWS/EC2',
            metric='CPUUtilization', statistic='Average',
            comparison='<', threshold='25',
            period='240', evaluation_periods=1,
            alarm_actions=[scale_down_policy.policy_arn],
            dimensions=alarm_dimensions)
cloudwatch.create_alarm(scale_down_alarm)

time.sleep(60)

i = 0
while i < 3:
  req = "http://" + load_generator.public_dns_name + "/warmup"
  response = requests.get(req, params={"dns": load_balancer.dns_name})
  time.sleep(5*60+30)

req = "http://" + load_generator.public_dns_name + "/autoscale"
response = requests.get(req, params={"dns": load_balancer.dns_name})


#release all resources
time.sleep(120)

ag.shutdown_instances()
ag.delete()

#lc.delete()

scale_up_policy.delete()
scale_down_policy.delete()

scale_up_alarm.delete()
scale_down_alarm.delete()

load_balancer.delete()