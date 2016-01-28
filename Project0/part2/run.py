import boto.ec2
import time
import re
import requests
import os

Load_Generator = "ami-76164d1c"
Load_Generator_Type = "m3.medium"

Data_Center = "ami-f4144f9e"
Data_Center_Type = "m3.medium"

ACCESS_KEY_ID = os.environ('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.environ('SECRET_ACCESS_KEY')

SUBMISSION_PASSWORD = "jEJEH6og330aT5TqlrYAIMFNqDrqflW4"

SECURITY_GROUP_ID = "sg-091a7270"
SECURITY_GROUP = ['launch-wizard-7']
KEY_NAME = "P0.1LoadGenerator"

LOAD_THRESLOAD = 4000

# Create a connection
conn = boto.ec2.connect_to_region("us-east-1", aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY)


def terminate_data_centers(instance_list):
    for instance in instance_list:
        instance.terminate()


def launch_instance(AMI_ID, instance_type, name):
    reservation = conn.run_instances(AMI_ID, key_name=KEY_NAME, instance_type=instance_type, security_groups=SECURITY_GROUP)

    instance = reservation.instances[0]
    stat = instance.update()
    while stat == "pending":
        time.sleep(20)
        stat = instance.update()

    if stat == "running":
        instance.add_tag("Name", name)
    time.sleep(80)
    return instance


def set_password(dns):
    http_request = "http://" + dns + "/password"
    response = requests.get(http_request, params={"passwd": SUBMISSION_PASSWORD})
    time_interval = 0
    # see if set password successfully.
    # if see "return to homepage in body, then succeed"
    # else retry
    while "return to the homepage" not in response.text:
        if time_interval > 10:
            return
        response = requests.get(http_request, params={"passwd": SUBMISSION_PASSWORD})
        if "return to the homepage" in response.text:
            return

        time_interval += 1
        time.sleep(10)

    return


def activate_data_center(load_generator_dns, data_center_dns, flag):
    req = "http://" + load_generator_dns + "/test/horizontal"
    if not flag:
        req += "/add"

    time_interval = 0
    while True:
        if time_interval > 10:
            break
        time.sleep(10)
        response = requests.get(req, params={"dns": data_center_dns})
        time_interval += 1
        stat = re.search(r'test\.\d{1,}\.log', response.text)

        if stat != None:
            log_id = stat.group()
            return log_id

    print "fail to activate data center instance"
    return 0


def crawl_parse_log(load_generator_dns, log_name):
    req = "http://" + load_generator_dns + "/log"

    req_param = {"name": "test." + log_name + ".log"}
    response = requests.get(req, params=req_param)
    lines = response.text.split("\n")

    last_line = len(lines)
    rps = 0
    for i in reversed(range(0, last_line - 1)):
        line = lines[i]
        if line == "":
            continue
        if line.startswith("ec2"):
            rps += float(line.split("=")[1])
        elif line.startswith("[Min"):
            break
    print "Current rps: " + str(rps)
    return rps


instance_list = []
load_generator = launch_instance(Load_Generator, Load_Generator_Type, "Load Generator")
instance_list.append(load_generator)

set_password(load_generator.public_dns_name)


data_center_index = 0

data_center_one = launch_instance(Data_Center, Data_Center_Type, "DataCenter-" + str(data_center_index))
instance_list.append(data_center_one)
print "Create " + data_center_one.public_dns_name
data_center_index += 1

log_id = activate_data_center(load_generator.public_dns_name, data_center_one.public_dns_name, True)

load = crawl_parse_log(load_generator.public_dns_name, str(log_id))

while load < LOAD_THRESLOAD:
    data_center_instance = launch_instance(Data_Center, Data_Center_Type, "Data Center-" + str(data_center_index))
    instance_list.append(data_center_instance)
    data_center_index += 1
    log_id = activate_data_center(load_generator.public_dns_name, data_center_instance.public_dns_name,
                                       False)

    load = crawl_parse_log(load_generator.public_dns_name, str(log_id))

print "total data centers " + str(data_center_index)

# terminate all data center instances
terminate_data_centers(instance_list)
