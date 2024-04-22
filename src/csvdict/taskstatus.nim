import std/[algorithm, math, options, sequtils]
import std/[times, streams, strutils, parseutils]
import std/importutils
import ../csvdict {.all.}
import ../csvdict/csvdictsnapshot {.all.}

export csvdictsnapshot, times

type
    TaskStatus* = object
        dict: CsvDictSnapshotWithMainColumn
        timeOfSnapshot: int

    Column* = enum
        Task, Status, LastSuccess, Frequency

    StatusVal* = enum
        Success, Fail

    TaskVal = object
        strVal: string
        statusVal: StatusVal
        dateTimeVal: DateTime
        durationVal: Duration

    TaskRow* = object
        data: seq[string]

privateAccess(CsvDict) # For rows access

const
    MainColumn = Task
    DateFormat = "yyyy-MM-dd'_'HH'h'"
    ColumnIdealSize = @[25, 15, 20, 15]


proc init*(T: type TaskStatus, fileTimeStamp = -1): TaskStatus
# Io
proc readTaskStatus*(input: Stream, fileTimeStamp: int): TaskStatus
proc checkIfMustUpdate*(taskStatus: TaskStatus, fileTimeStamp: int): bool
proc update*(taskStatus: var TaskStatus, input: Stream, fileTimeStamp: int)
proc writeTaskStatus*(taskStatus: var TaskStatus, output: Stream, fileTimeStamp: int,
    sortFn: proc(a, b: TaskRow): int = nil, sortOrder = SortOrder.Ascending)
# TaskStatus: Seq like operations
proc add*(self: var TaskStatus, row: TaskRow)
# TaskRow
proc toTaskRow*(task: string, status: StatusVal, lastSuccess: DateTime, frequency: Duration): TaskRow
proc toStr*(row: TaskRow): seq[string]
func toTaskVal*(strVal: string): TaskVal
func toTaskVal*(statusVal: StatusVal): TaskVal
func toTaskVal*(dateTimeVal: DateTime): TaskVal
func toTaskVal*(durationVal: Duration): TaskVal
proc toStr(val: TaskVal, column: Column): string
#func `[]`*(row: TaskRow, column: static Column): auto
# TaskStatus: Table like operations
func find*(self: TaskStatus, task: string): Option[RowPos]
func `[]`*(self: TaskStatus, rowPos: RowPos): TaskRow
#proc `[]`*(self: TaskStatus, rowPos: RowPos, column: static Column): auto
func `[]`*(self: TaskStatus, task: string): TaskRow
#func `[]`*(self: TaskStatus, task: string, column: static Column): auto
proc `[]=`*(self: var TaskStatus, task: string, newRow: TaskRow)
proc `[]=`*(self: var TaskStatus, task: string, column: Column, val: TaskVal)
proc update*(self: var TaskStatus, task: string, values: openarray[(Column, TaskVal)])
proc update*(self: var TaskStatus, rowPos: RowPos, values: openarray[(Column, TaskVal)])
# Utilities
func toHeader(T: type Column): seq[string]
func parseStatus*(val: string): StatusVal
proc parseTime*(val: string): DateTime
proc formatTime(time: DateTime): string
func parseFrequency*(val: string): Duration
func formatFrequency(frequency: Duration): string
proc sortImpl(csvDict: var CsvDict, comparator: proc(a, b: TaskRow): int, order = SortOrder.Ascending)
# High level operations
proc updateTask*(self: var TaskStatus, task: string, status: StatusVal)
proc isLate*(taskRow: TaskRow): bool


proc init*(T: type TaskStatus, fileTimeStamp = -1): TaskStatus =
    TaskStatus(
        dict: CsvDict.init(Column.toHeader()).toSnapshot($MainColumn),
        timeOfSnapshot: fileTimeStamp
    )

# Io
proc readTaskStatus*(input: Stream, fileTimeStamp: int): TaskStatus =
    let csvDictOpt = readCsv(input, skipInitialSpace = true)
    if csvDictOpt.isNone(): raise newException(OsError, "Couldn't read taskstatus")
    let csvDict = csvDictOpt.get()
    if csvDict.header != Column.toHeader(): raise newException(OsError, "Header don't correspond")
    return TaskStatus(
        dict: csvDict.toSnapshot($MainColumn),
        timeOfSnapshot: fileTimeStamp
    )

proc checkIfMustUpdate*(taskStatus: TaskStatus, fileTimeStamp: int): bool =
    taskStatus.timeOfSnapshot != fileTimeStamp

proc update*(taskStatus: var TaskStatus, input: Stream, fileTimeStamp: int) =
    let csvDict = readCsv(input, skipInitialSpace = true)
    if csvDict.isNone(): raise newException(OsError, "Couldn't read taskstatus")
    taskStatus.dict.updateBaseDict(csvDict.get())
    taskStatus.timeOfSnapshot = fileTimeStamp

proc writeTaskStatus*(taskStatus: var TaskStatus, output: Stream, fileTimeStamp: int,
                    sortFn: proc(a, b: TaskRow): int = nil, sortOrder = SortOrder.Ascending) =
    if taskStatus.timeOfSnapshot != fileTimeStamp:
        raise newException(IOError, "File has been modified since")
    var csvDictReconciled = taskStatus.dict.reconcileChange()
    if sortFn != nil:
        csvDictReconciled.sortImpl(sortFn, sortOrder)
    csvDictReconciled.writeCsv(output, rowMinSizes = ColumnIdealSize)

# TaskStatus: Seq like operations
proc add*(self: var TaskStatus, row: TaskRow) =
    self.dict.add row.data

# TaskRow
proc toTaskRow*(task: string, status: StatusVal, lastSuccess: DateTime, frequency: Duration): TaskRow =
    TaskRow(data: @[
        task,
        $status,
        lastSuccess.formatTime(),
        frequency.formatFrequency(),
    ])

proc toStr*(row: TaskRow): seq[string] =
    row.data

func toTaskVal*(strVal: string): TaskVal =
    TaskVal(strVal: strVal)

func toTaskVal*(statusVal: StatusVal): TaskVal =
    TaskVal(statusVal: statusVal)

func toTaskVal*(dateTimeVal: DateTime): TaskVal =
    TaskVal(dateTimeVal: dateTimeVal)

func toTaskVal*(durationVal: Duration): TaskVal =
    TaskVal(durationVal: durationVal)

proc toStr(val: TaskVal, column: Column): string =
    case column:
    of Task:
        return val.strVal
    of Status:
        return $val.statusVal
    of LastSuccess:
        return val.dateTimeVal.formatTime()
    of Frequency:
        return val.durationVal.formatFrequency()

proc `[]`*(row: TaskRow, column: static Column): auto =
    when column == Task:
        return row.data[column.int]
    elif column == Status:
        return row.data[column.int].parseStatus()
    elif column == LastSuccess:
        return row.data[column.int].parseTime()
    elif column == Frequency:
        return row.data[column.int].parseFrequency()


# TaskStatus: Table like operations
func find*(self: TaskStatus, task: string): Option[RowPos] =
    self.dict.find(task)

func `[]`*(self: TaskStatus, rowPos: RowPos): TaskRow =
    TaskRow(data: self.dict[rowPos])

proc `[]`*(self: TaskStatus, rowPos: RowPos, column: static Column): auto =
    self[rowPos][column]

func `[]`*(self: TaskStatus, task: string): TaskRow =
    TaskRow(data: self.dict[task])

proc `[]`*(self: TaskStatus, task: string, column: static Column): auto =
    self[task][column]

proc `[]=`*(self: var TaskStatus, task: string, newRow: TaskRow) =
    self.dict[task] = newRow.data

proc `[]=`*(self: var TaskStatus, task: string, column: Column, val: TaskVal) =
    self.dict.update(task, {$column: val.toStr(column)})

proc update*(self: var TaskStatus, task: string, values: openarray[(Column, TaskVal)]) =
    var valuesString: seq[(string, string)]
    for valWithCol in values:
        valuesString.add ($valWithCol[0], valWithCol[1].toStr(valWithCol[0]))
    self.dict.update(task, valuesString)

proc update*(self: var TaskStatus, rowPos: RowPos, values: openarray[(Column, TaskVal)]) =
    var valuesString: seq[(string, string)]
    for valWithCol in values:
        valuesString.add ($valWithCol[0], valWithCol[1].toStr(valWithCol[0]))
    self.dict.update(rowPos, valuesString)


# Utilities
func toHeader(T: type Column): seq[string] =
    for item in T.items():
        result.add $item

func parseStatus(val: string): StatusVal =
    if val != $Fail:
        Success
    else:
        Fail

proc parseTime*(val: string): DateTime =
    times.parse(val, DateFormat)

proc formatTime(time: DateTime): string =
    times.format(time, DateFormat)

func parseFrequency*(val: string): Duration =
    var
        daysInt: int
        hoursInt: int
    let length = parseInt(val, daysInt)
    discard parseInt(val[length - 1 + 2 .. ^1], hoursInt)
    initDuration(hours = daysInt * 24 + hoursInt)

func formatFrequency(frequency: Duration): string =
    let (days, hours) = divmod(frequency.inHours(), 24)
    return $days & "d_" & hours.intToStr(2) & "h"

proc sortImpl(csvDict: var CsvDict, comparator: proc(a, b: TaskRow): int, order = SortOrder.Ascending) =
    ## Ensure dict is up to date !
    csvDict.rows.sort(proc(rowA, rowB: seq[string]): int =
        return comparator(TaskRow(data: rowA), TaskRow(data: rowB))
    , order)

# High level operations
proc updateTask*(self: var TaskStatus, task: string, status: StatusVal) =
    ## update lastSuccess time accordingly
    if status == Success:
        self.update(task, { Status: toTaskVal(Success), LastSuccess: toTaskVal(now()) })
    else:
        self.update(task, { Status: toTaskVal(Fail) })

proc isLate*(taskRow: TaskRow): bool =
    let
        lastSuccess = taskRow[LastSuccess]
        frequency = taskRow[Frequency]
    (now() - lastSuccess) > frequency
