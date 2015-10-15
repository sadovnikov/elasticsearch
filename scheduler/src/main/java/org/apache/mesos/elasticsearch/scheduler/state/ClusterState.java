package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.elasticsearch.scheduler.Task;

import java.io.IOException;
import java.io.NotSerializableException;
import java.security.InvalidParameterException;
import java.util.*;

import static org.apache.mesos.Protos.TaskID;

/**
 * Model of cluster state. User is able to add, remove and monitor task status.
 */
public class ClusterState {
    public static final Logger LOGGER = Logger.getLogger(ClusterState.class);
    public static final String STATE_LIST = "stateList";
    private SerializableState zooKeeperStateDriver;
    private FrameworkState frameworkState;

    public ClusterState(SerializableState zooKeeperStateDriver, FrameworkState frameworkState) {
        this.zooKeeperStateDriver = zooKeeperStateDriver;
        this.frameworkState = frameworkState;
        frameworkState.onStatusUpdate(this::updateTask);
    }

    /**
     * Get a list of all tasks with state
     * @return a list of TaskInfo
     */
    // Makes:
    //   1 GET request to "/frameworkId"
    //   1 GET request to "/" ++ F ++ "/stateList"
    public List<TaskInfo> getTaskList() {
        List<TaskInfo> taskInfoList = null;
        try {
            // getKey() makes 1 GET request to "/frameworkId"
            // then we make 1 GET request to "/" ++ F ++ "/stateList"
            taskInfoList = zooKeeperStateDriver.get(getKey());
        } catch (IOException e) {
            LOGGER.info("Unable to get key for cluster state due to invalid frameworkID.", e);
        }
        return taskInfoList == null ? new ArrayList<>(0) : taskInfoList;
    }

    /**
     * Get a list of all tasks in a format specific to the web GUI.
     * @return
     */
    // Where the DB currently has frameworkId F, with N tasks, this makes
    //   2N+1 GET requests to "/frameworkId"
    //   N+1 GET requests to "/" ++ F ++ "/stateList"
    //   N GET requests to "/" ++ frameworkID ++ "/state/" ++ taskID
    public Map<String, Task> getGuiTaskList() {
        Map<String, Task> tasks = new HashMap<>();
        // getTaskList() makes
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        // where this returns N tasks.

        // then for each of these tasks it makes:
        //   2 GET requests to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        //   1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID
        getTaskList().forEach(taskInfo -> tasks.put(taskInfo.getTaskId().getValue(), Task.from(taskInfo, getStatus(taskInfo.getTaskId()).getStatus())));
        return tasks;
    }

    /**
     * Get the status of a specific task
     * @param taskID the taskID to retrieve the task status for
     * @return a POJO representing TaskInfo, TaskStatus and FrameworkID packets
     * @throws InvalidParameterException when the taskId does not exist in the Task list.
     */
    // Makes
    //   2 GET requests to "/frameworkId"
    //   1 GET request to "/" ++ F ++ "/stateList"
    //   if task is already in ZK:
    //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID
    //   else:
    //     1 GET request to "/" ++ frameworkID
    //     1 GET request to "/" ++ frameworkID ++ "/state"
    //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID
    //     [0,3] SET requests to above components
    //     1 SET request to "/" ++ frameworkID ++ "/state/" ++ taskID
    public ESTaskStatus getStatus(TaskID taskID) throws IllegalArgumentException {
        // getTask(taskID) makes
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"

        // getStatus makes
        //   1 GET request to "/frameworkId"
        //   if task is already in ZK:
        //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID
        //   else:
        //     1 GET request to "/" ++ frameworkID
        //     1 GET request to "/" ++ frameworkID ++ "/state"
        //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID
        //     [0,3] SET requests to above components
        //     1 SET request to "/" ++ frameworkID ++ "/state/" ++ taskID
        return getStatus(getTask(taskID));
    }

    // Makes
    //   1 GET request to "/frameworkId"
    //   if task is already in ZK:
    //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
    //   else:
    //     1 GET request to "/" ++ frameworkID
    //     1 GET request to "/" ++ frameworkID ++ "/state"
    //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
    //     [0,3] SET requests to above components
    //     1 SET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
    private ESTaskStatus getStatus(TaskInfo taskInfo) {
        // frameworkState.getFrameworkID() makes:
        //   1 GET request to /frameworkId

        // `new ESTaskStatus(...)` makes:
        //   if task is already in ZK:
        //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
        //   else:
        //     1 GET request to "/" ++ frameworkID
        //     1 GET request to "/" ++ frameworkID ++ "/state"
        //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
        //     [0,3] SET requests to above components
        //     1 SET request to "/" ++ frameworkID ++ "/state/" ++ taskInfo.getTaskId()
        return new ESTaskStatus(zooKeeperStateDriver, frameworkState.getFrameworkID(), taskInfo, new StatePath(zooKeeperStateDriver));
    }

    // makes
    //   4 GET requests to "/frameworkId"
    //   1 GET request  to "/" ++ frameworkID
    //   3 GET requests to "/" ++ frameworkID ++ "/stateList"
    //   1 SET request  to "/" ++ frameworkID ++ "/stateList"
    //   another [0,2] SET requests
    public void addTask(ESTaskStatus esTask) {
        addTask(esTask.getTaskInfo());
    }

    // makes
    //   4 GET requests to "/frameworkId"
    //   1 GET request  to "/" ++ frameworkID
    //   3 GET requests to "/" ++ frameworkID ++ "/stateList"
    //   1 SET request  to "/" ++ frameworkID ++ "/stateList"
    //   another [0,2] SET requests
    public void addTask(TaskInfo taskInfo) {
        LOGGER.debug("Adding TaskInfo to cluster for task: " + taskInfo.getTaskId().getValue());

        // exists(...) makes:
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ frameworkID ++ "/stateList"
        if (exists(taskInfo.getTaskId())) {
            // FIXME this is stupid; if it already exists then this should be an error. Assume this doesn't happen.

            // removeTask(...) makes:
            //   4 GET requests to "/frameworkId"
            //   2 GET requests to "/" ++ frameworkID ++ "/stateList"
            //   1 SET request  to "/" ++ frameworkID ++ "/stateList"
            //   1 GET request  to "/" ++ frameworkID ++ "/state/" ++ taskID
            //   1 DEL request  to "/" ++ frameworkID ++ "/state/" ++ taskID
            //   1 GET request  to "/" ++ frameworkID
            //   another [0,2] SET requests
            removeTask(taskInfo);
        }

        // getTaskList() makes:
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        List<TaskInfo> taskList = getTaskList();

        taskList.add(taskInfo);

        // setTaskInfoList() makes
        //   2 GET requests to "/frameworkId"
        //   1 GET request to "/" ++ F
        //   1 GET request to "/" ++ F ++ "/stateList"
        //   1 SET request to "/" ++ F ++ "/stateList"
        //   another [0,2] SET requests
        setTaskInfoList(taskList);
    }

    // Makes:
    //   4 GET requests to "/frameworkId"
    //   2 GET requests to "/" ++ frameworkID ++ "/stateList"
    //   1 SET request  to "/" ++ frameworkID ++ "/stateList"
    //   1 GET request  to "/" ++ frameworkID ++ "/state/" ++ taskID
    //   1 DEL request  to "/" ++ frameworkID ++ "/state/" ++ taskID
    //   1 GET request  to "/" ++ frameworkID
    //   another [0,2] SET requests
    public void removeTask(TaskInfo taskInfo) throws InvalidParameterException {
        // getTaskList() makes:
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        List<TaskInfo> taskList = getTaskList();

        LOGGER.debug("Removing TaskInfo from cluster for task: " + taskInfo.getTaskId().getValue());

        if (!taskList.remove(taskInfo)) {
            throw new InvalidParameterException("TaskInfo does not exist in list: " + taskInfo.getTaskId().getValue());
        }

        // If we're here, the task is in ZK

        // getStatus(...) makes:
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID

        // .destroy() makes:
        //   1 DELETE request to "/" ++ frameworkID ++ "/state/" ++ taskID
        getStatus(taskInfo).destroy(); // Destroy task status in ZK.

        // makes
        //   2 GET requests to "/frameworkId"
        //   1 GET request to "/" ++ frameworkID
        //   1 GET request to "/" ++ frameworkID ++ "/stateList"
        //   1 SET request to "/" ++ frameworkID ++ "/stateList"
        //   another [0,2] SET requests
        setTaskInfoList(taskList); // Remove from cluster state list
    }

    // makes:
    //   1 GET request to "/frameworkId"
    //   1 GET request to "/" ++ F ++ "/stateList"
    public Boolean exists(TaskID taskId) {
        try {
            getTask(taskId);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    /**
     * Get the TaskInfo packet for a specific task.
     * @param taskID the taskID to retrieve the TaskInfo for
     * @return a TaskInfo packet
     * @throws IllegalArgumentException when the taskId does not exist in the Task list.
     */
     // makes:
     //   1 GET request to "/frameworkId"
     //   1 GET request to "/" ++ F ++ "/stateList"
     private TaskInfo getTask(TaskID taskID) throws IllegalArgumentException {
        // getTaskList() makes
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        List<TaskInfo> taskInfoList = getTaskList();

        LOGGER.debug("Getting task " + taskID.getValue() + ", from " + logTaskList(taskInfoList));

        TaskInfo taskInfo = null;

        for (TaskInfo info : taskInfoList) {
            LOGGER.debug("Testing: " + info.getTaskId().getValue() + " .equals " + taskID.getValue() + " = " + info.getTaskId().getValue().equals(taskID.getValue()));

            if (info.getTaskId().getValue().equals(taskID.getValue())) {
                taskInfo = info;
                break;
            }
        }
        if (taskInfo == null) {
            throw new IllegalArgumentException("Could not find executor with that task ID: " + taskID.getValue());
        }
        return taskInfo;
    }

    public TaskInfo getTask(Protos.ExecutorID executorID) throws IllegalArgumentException {
        if (executorID.getValue().isEmpty()) {
            throw new IllegalArgumentException("ExecutorID.value() is blank. Cannot be blank.");
        }
        List<TaskInfo> taskInfoList = getTaskList();
        LOGGER.debug("Getting task " + executorID.getValue());
        TaskInfo taskInfo = null;
        for (TaskInfo info : taskInfoList) {
            LOGGER.debug("Testing: " + info.getExecutor().getExecutorId().getValue() + " .equals " + executorID.getValue() + " = " + info.getExecutor().getExecutorId().getValue().equals(executorID.getValue()));
            if (info.getExecutor().getExecutorId().getValue().equals(executorID.getValue())) {
                taskInfo = info;
                break;
            }
        }
        if (taskInfo == null) {
            throw new IllegalArgumentException("Could not find executor with that executor ID: " + executorID.getValue());
        }
        return taskInfo;
    }

    // Makes:
    //   3 GET requests to "/frameworkId"
    //   2 GET requests to "/" ++ F ++ "/stateList"
    //   1 GET request to "/" ++ F ++ "/state/" ++ taskID
    //   1 SET request to "/" ++ F ++ "/state/" ++ taskID
    //   3 more GET requests
    private void update(Protos.TaskStatus status) throws IllegalArgumentException {
        // exists(...) makes:
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        if (!exists(status.getTaskId())) {
            throw new IllegalArgumentException("Task does not exist in zk.");
        }

        // Since we know the task exists at this point, getStatus(...) makes:
        //   2 GET requests to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        //   1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID

        // setStatus(...) makes:
        //   3 GET requests
        //   1 SET request to F ++ "/state/" ++ taskID
        getStatus(status.getTaskId()).setStatus(status);
    }

    // Precondition: task exists
    // Makes:
    //   2 GET requests to "/frameworkId"
    //   1 GET request to "/" ++ F ++ "/stateList"
    //   2 GET requests to "/" ++ frameworkID ++ "/state/" ++ taskID
    public boolean taskInError(Protos.TaskStatus status) {
        // getStatus(...) makes
        //   2 GET requests to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        //   if task is already in ZK:
        //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID
        //   else:
        //     1 GET request to "/" ++ frameworkID
        //     1 GET request to "/" ++ frameworkID ++ "/state"
        //     1 GET request to "/" ++ frameworkID ++ "/state/" ++ taskID
        //     [0,3] SET requests to above components
        //     1 SET request to "/" ++ frameworkID ++ "/state/" ++ taskID

        // .taskInError() makes:
        //   1 GET request to frameworkID ++ "/state/" ++ taskID
        return getStatus(status.getTaskId()).taskInError();
    }

    /**
     * Updates a task with the given status. Status is written to zookeeper.
     * If the task is in error, then the healthchecks are stopped and state is removed from ZK
     * @param status A received task status
     */
    // Makes:
    //   7 GET requests to "/frameworkId"
    //   5 GET requests to "/" ++ F ++ "/stateList"
    //   3 GET requests to "/" ++ frameworkID ++ "/state/" ++ taskID
    //   1 SET request  to "/" ++ frameworkID ++ "/state/" ++ taskID
    //   3 more GET requests
    //
    //   and if the task is in error, also makes:
    //   4 GET requests to "/frameworkId"
    //   2 GET requests to "/" ++ frameworkID ++ "/stateList"
    //   1 SET request  to "/" ++ frameworkID ++ "/stateList"
    //   1 GET request  to "/" ++ frameworkID ++ "/state/" ++ taskID
    //   1 DEL request  to "/" ++ frameworkID ++ "/state/" ++ taskID
    //   1 GET request  to "/" ++ frameworkID
    //   another [0,2] SET requests
    private void updateTask(Protos.TaskStatus status) {

        // exists(..) makes:
        //   1 GET request to "/frameworkId"
        //   1 GET request to "/" ++ F ++ "/stateList"
        if (!exists(status.getTaskId())) {
            // Assume this doesn't happen
            LOGGER.warn("Could not find task in cluster state.");
            return;
        }

        try {
            // getTask(...) makes:
            //   1 GET request to "/frameworkId"
            //   1 GET request to "/" ++ F ++ "/stateList"
            Protos.TaskInfo taskInfo = getTask(status.getTaskId());

            LOGGER.debug("Updating task status for executor: " + status.getExecutorId().getValue() + " [" + status.getTaskId().getValue() + ", " + status.getTimestamp() + ", " + status.getState() + "]");

            // Makes:
            //   3 GET requests to "/frameworkId"
            //   2 GET requests to "/" ++ F ++ "/stateList"
            //   1 GET request to "/" ++ F ++ "/state/" ++ taskID
            //   1 SET request to "/" ++ F ++ "/state/" ++ taskID
            //   3 more GET requests
            update(status); // Update state of Executor

            // taskInError(...) makes:
            //   2 GET requests to "/frameworkId"
            //   1 GET request to "/" ++ F ++ "/stateList"
            //   2 GET requests to "/" ++ frameworkID ++ "/state/" ++ taskID
            if (taskInError(status)) {
                LOGGER.error("Task in error state. Removing state for executor: " + status.getExecutorId().getValue() + ", due to: " + status.getState());
                // removeTask(...) makes:
                //   4 GET requests to "/frameworkId"
                //   2 GET requests to "/" ++ frameworkID ++ "/stateList"
                //   1 SET request  to "/" ++ frameworkID ++ "/stateList"
                //   1 GET request  to "/" ++ frameworkID ++ "/state/" ++ taskID
                //   1 DEL request  to "/" ++ frameworkID ++ "/state/" ++ taskID
                //   1 GET request  to "/" ++ frameworkID
                //   another [0,2] SET requests
                removeTask(taskInfo); // Remove task from cluster state.
            }
        } catch (IllegalStateException | IllegalArgumentException e) {
            LOGGER.error("Unable to write executor state to zookeeper", e);
        }
    }

    private String logTaskList(List<TaskInfo> taskInfoList) {
        List<String> res = new ArrayList<>();
        for (TaskInfo t : taskInfoList) {
            res.add(t.getTaskId().getValue());
        }
        return Arrays.toString(res.toArray());
    }

    // makes
    //   2 GET requests to /frameworkId
    //   1 GET request to "/" ++ F
    //   1 GET request to "/" ++ F ++ "/stateList"
    //   1 SET request to "/" ++ F ++ "/stateList"
    //   another [0,2] SET requests
    private void setTaskInfoList(List<TaskInfo> taskInfoList) {
        LOGGER.debug("Writing executor state list: " + logTaskList(taskInfoList));
        try {
            // getKey makes:
            //   1 request to /frameworkId

            // mkdir makes:
            //   1 GET request to "/" ++ F
            //   1 GET request to "/" ++ F ++ "/stateList"
            //   [0,2] SET requests
            new StatePath(zooKeeperStateDriver).mkdir(getKey());

            // getKey makes:
            //   1 request to /frameworkId

            // set makes
            //   1 SET request to "/" ++ F ++ "/stateList"
            zooKeeperStateDriver.set(getKey(), taskInfoList);
        } catch (IOException ex) {
            LOGGER.error("Could not write list of executor states to zookeeper: ", ex);
        }
    }

    /**
     * REQUESTS:
     *   1 request to /frameworkId
     *
     * Return value has 2 components
     */
    private String getKey() throws NotSerializableException {
        return frameworkState.getFrameworkID().getValue() + "/" + STATE_LIST;
    }
}
