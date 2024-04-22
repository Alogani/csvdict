import csvdict, csvdict/taskstatus
import std/unittest

import std/[options, streams, strutils]


test "getters":
    var taskList = readTaskStatus(
        newStringStream("""
            Task;Status;LastSuccess;Frequency
            Backup;Success;2023-01-01_00h;7d_00h
            Update;Fail;1970-01-01_00h;7d_00h
            BackupOld;Success;2020-01-01_00h;7d_00h
        """), fileTimeStamp = -1)
    check taskList["Backup"].toStr() == @["Backup", "Success", "2023-01-01_00h", "7d_00h"]
    check taskList["Backup", Status] == Success
    check taskList["Backup", LastSuccess] == dateTime(2023, mJan, 01)
    check taskList["Update", Frequency].inDays() == 7
    let updateTaskIdx = taskList.find("Update")
    check updateTaskIdx.isSome()
    check taskList[updateTaskIdx.get(), LastSuccess] == dateTime(1970, mJan, 01)
    var outStream = newStringStream()
    taskList.writeTaskStatus(outStream, fileTimeStamp = -1, sortFn = proc(a, b: TaskRow): int =
        (a[LastSuccess] - b[LastSuccess]).inHours()
    )
    outStream.setPosition(0)
    check outStream.readAll().replace(" ") == """Task;               Status;             LastSuccess;        Frequency;
Update;             Fail;               1970-01-01_00h;     7d_00h;
BackupOld;          Success;            2020-01-01_00h;     7d_00h;
Backup;             Success;            2023-01-01_00h;     7d_00h;
""".replace(" ")


test "setters":
    var taskList = TaskStatus.init()
    taskList.add toTaskRow("Backup", Success, dateTime(2023, mJan, 01), initDuration(days = 7))
    check taskList["Backup"].toStr() == @["Backup", "Success", "2023-01-01_00h", "7d_00h"]
    taskList["Backup", Task] = "Update".toTaskVal
    taskList["Update", Status] = Fail.toTaskVal
    taskList["Update", LastSuccess] = dateTime(2024, mJan, 01).toTaskVal
    taskList["Update", Frequency] = initDuration(days = 5).toTaskVal
    check taskList["Update"].toStr() == @["Update", "Fail", "2024-01-01_00h", "5d_00h"]
    taskList.update("Update", {Task: "Backup".toTaskVal, Status: Success.toTaskVal})
    check taskList["Backup"].toStr() == @["Backup", "Success", "2024-01-01_00h", "5d_00h"]