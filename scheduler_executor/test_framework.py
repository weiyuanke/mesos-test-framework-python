#!/usr/bin/env python
#-*- coding:utf8
import os
import sys
import time
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

TOTAL_TASKS = 5
TASK_CPUS = 1
TASK_MEM = 128

class TestScheduler(mesos.interface.Scheduler):
    def __init__(self, executor):
        self.executor = executor
        self.taskData = {}
        self.tasksLaunched = 0
        self.messagesReceived = 0

    #框架注册的回调
    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    #scheduler接收到mesos的resource offer
    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value
            #接收到的总的资源数量
            print "Received offer %s with cpus: %s and mem: %s" % (offer.id.value, offerCpus, offerMem)
            remainingCpus = offerCpus
            remainingMem = offerMem

            #发起任务
            while self.tasksLaunched < TOTAL_TASKS and \
                  remainingCpus >= TASK_CPUS and \
                  remainingMem >= TASK_MEM:
                tid = self.tasksLaunched
                self.tasksLaunched += 1
                print "Launching task %d using offer %s" % (tid, offer.id.value)

                #设置任务,一个executor包含了多个task
		#TaskInfo必须至少包含一个ExecutorInfo或者CommandInfo
                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "task %d" % tid
                task.executor.MergeFrom(self.executor)

                #任务所需的资源
                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = TASK_CPUS

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = TASK_MEM

                tasks.append(task)
                self.taskData[task.task_id.value] = (offer.slave_id, task.executor.executor_id)

                remainingCpus -= TASK_CPUS
                remainingMem -= TASK_MEM

            operation = mesos_pb2.Offer.Operation()
            #operation的type为LAUNCH\RESERVE\UNRESERVE\CREATE\DESTROY
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            # Accepts the given offers and performs a sequence of operations on those accepted offers.
            driver.acceptOffers([offer.id], [operation])

    #该函数是task 和scheduler通信的接口，per task
    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))
        slave_id, executor_id = self.taskData[update.task_id.value]
        if update.state == mesos_pb2.TASK_FINISHED:
            driver.sendFrameworkMessage(executor_id, slave_id, 'data with a \0 byte')
        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            print "Aborting because task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
            driver.abort()

    #该函数是executor和scheduler通信的结构，per executor
    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1
        if self.messagesReceived == TOTAL_TASKS:
            print "All tasks done, and all messages received, exiting"
            driver.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    #mesos_pb2: mesos protocol buffer
    #ExecutorInfo定义在mesos.proto文件中,描述了一个executor
    executor = mesos_pb2.ExecutorInfo()
    #每个executor必须有一个ExecutorId和CommandInfo
    executor.executor_id.value = "default"
    #command的类型为"CommandInfo",
    #executor要执行的python程序，因为是单机版安装，这里传递的是一个本地绝对路径。
    #如果是分布式部署的话，应该是一个hdfs的地址，或者网络上的地址，以便slave下载
    executor.command.value = os.path.abspath("./test_executor.py")
    #executor.command.value = "sleep 10"
    executor.name = "Test Executor (Python)"
    executor.source = "python_test"

    #FrameworkInfo描述了一个framework
    framework = mesos_pb2.FrameworkInfo()
    #必须有user和name，user表示executor、task以那个用户的身份运行,默认当前用户
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Test Framework (Python)"
    #If set, framework pid, executor pids and status updates are
    #checkpointed to disk by the slaves. Checkpointing allows a
    #restarted slave to reconnect with old executors and recover
    #status updates, at the cost of disk I/O.
    framework.checkpoint = True

    driver = mesos.native.MesosSchedulerDriver(TestScheduler(executor),framework,sys.argv[1])
    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
    # Ensure that the driver process terminates.
    driver.stop();
    sys.exit(status)
