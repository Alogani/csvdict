import ../csvdict {.all.}
import std/options

type
    CsvDictSnapshot* = object of RootObj
        ## A version of CsvDict with limited capabilities, but which can be reconciled
        oldDict: CsvDict
        newDict: CsvDict

    CsvDictSnapshotWithMainColumn* = object of CsvDictSnapshot
        mainColIdx: int

    RowPos = object
        dictIdx: int
        rowIdx: int

# CsvDictSnapshot
proc toSnapshot*(oldDict: CsvDict): CsvDictSnapshot
proc updateBaseDict*(self: var CsvDictSnapshot, newOldDict: CsvDict)
proc reconcileChange*(self: var CsvDictSnapshot, keyColumns: seq[string]): CsvDict
proc header*(self: CsvDictSnapshot): seq[string] {.inline.}
proc add*(self: var CsvDictSnapshot, row: seq[string])
proc add*(self: var CsvDictSnapshot, row: openArray[ValueWithColName])
func find*(self: CsvDictSnapshot, keyDict: openArray[ValueWithColName]): Option[RowPos]
func `[]`*(self: CsvDictSnapshot, rowPos: RowPos): seq[string]
func `[]`*(self: CsvDictSnapshot, keyDict: openarray[ValueWithColName]): seq[string]
func `[]`*(self: CsvDictSnapshot, keyDict: openarray[ValueWithColName], column: string): string
proc update*(self: var CsvDictSnapshot, rowPos: RowPos, values: openarray[ValueWithColName])
proc `[]=`*(self: var CsvDictSnapshot, keyDict: openarray[ValueWithColName], column: string, value: string)
proc `[]=`*(self: var CsvDictSnapshot, keyDict: openarray[ValueWithColName], newRow: seq[string])
proc update*(self: var CsvDictSnapshot, keyDict: openarray[ValueWithColName], values: openarray[ValueWithColName])
func findRow(self: CsvDictSnapshot, keyDict: openarray[ValueWithColName]): RowPos
func findRowImpl(self: CsvDictSnapshot, keyIdx: seq[ValueWithColIdx]): RowPos {.inline.}

# CsvDictSnapshotWithMainColumn
proc toSnapshot*(oldDict: CsvDict, mainColumn: string): CsvDictSnapshotWithMainColumn
proc reconcileChange*(self: var CsvDictSnapshotWithMainColumn): CsvDict
func find*(self: CsvDictSnapshotWithMainColumn, key: string): Option[RowPos]
func `[]`*(self: CsvDictSnapshotWithMainColumn, key: string): seq[string]
func `[]`*(self: CsvDictSnapshotWithMainColumn, key: string, column: string): string
proc `[]=`*(self: var CsvDictSnapshotWithMainColumn, key: string, newRow: seq[string])
proc update*(self: var CsvDictSnapshotWithMainColumn, key: string, values: openarray[ValueWithColName])


# CsvDictSnapshot
proc toSnapshot*(oldDict: CsvDict): CsvDictSnapshot =
    CsvDictSnapshot(
        oldDict: oldDict,
        newDict: CsvDict(header: oldDict.header),
    )

proc updateBaseDict*(self: var CsvDictSnapshot, newOldDict: CsvDict) =
    self.oldDict = newOldDict

proc reconcileChange*(self: var CsvDictSnapshot, keyColumns: seq[string]): CsvDict =
    ## CsvDictSnapshot can still be used, CsvDict is returned by convenience
    self.oldDict.merge(self.newDict, keyColumns)
    return self.oldDict

proc header*(self: CsvDictSnapshot): seq[string] =
    self.oldDict.header

proc add*(self: var CsvDictSnapshot, row: seq[string]) =
    self.newDict.add row

proc add*(self: var CsvDictSnapshot, row: openArray[ValueWithColName]) =
    self.newDict.add row

func find*(self: CsvDictSnapshot, keyDict: openArray[ValueWithColName]): Option[RowPos] =
    ## Return an object that can be used directly inside `[]` or update to avoid searching twice
    let rowPos = self.findRow(keyDict)
    if rowPos.rowIdx == -1:
        return none(RowPos)
    return some(rowPos)

func `[]`*(self: CsvDictSnapshot, rowPos: RowPos): seq[string] =
    if rowPos.dictIdx == 0:
        return self.oldDict[rowPos.rowIdx]
    return self.newDict[rowPos.rowIdx]

func `[]`*(self: CsvDictSnapshot, keyDict: openarray[ValueWithColName]): seq[string] =
    self[self.findRow(keyDict)]

func `[]`*(self: CsvDictSnapshot, keyDict: openarray[ValueWithColName], column: string): string =
    self[keyDict][self.oldDict.findColumn(column)]

proc update*(self: var CsvDictSnapshot, rowPos: RowPos, values: openarray[ValueWithColName]) =
    var rowIdx = rowPos.rowIdx
    if rowPos.dictIdx == 0:
        self.newDict.add self.oldDict[rowIdx]
        rowIdx = self.newDict.len() - 1
    self.newDict.update(rowIdx, values)

proc `[]=`*(self: var CsvDictSnapshot, keyDict: openarray[ValueWithColName], column: string, value: string) =
    self.update(self.findRow(keyDict), {column: value})

proc `[]=`*(self: var CsvDictSnapshot, keyDict: openarray[ValueWithColName], newRow: seq[string]) =
    let rowPos = self.findRow(keyDict)
    if rowPos.dictIdx == 0:
        self.add newRow
    else:
        self.newDict[rowPos.rowIdx] = newRow

proc update*(self: var CsvDictSnapshot, keyDict: openarray[ValueWithColName], values: openarray[ValueWithColName]) =
    self.update(self.findRow(keyDict), values)

func findRow(self: CsvDictSnapshot, keyDict: openarray[ValueWithColName]): RowPos =
    return self.findRowImpl(self.oldDict.convertColNameToIdx(@keyDict))

func findRowImpl(self: CsvDictSnapshot, keyIdx: seq[ValueWithColIdx]): RowPos =
    var findRows = self.newDict.findRowsImpl(keyIdx, true)
    if findRows.len() == 0:
        findRows = self.oldDict.findRowsImpl(keyIdx, true)
        return RowPos(dictIdx: 0, rowIdx: if findRows.len() == 0: -1 else: findRows[0])
    return RowPos(dictIdx: 1, rowIdx: findRows[0])


# CsvDictSnapshotWithMainColumn
proc toSnapshot*(oldDict: CsvDict, mainColumn: string): CsvDictSnapshotWithMainColumn =
    CsvDictSnapshotWithMainColumn(
        oldDict: oldDict,
        newDict: CsvDict(header: oldDict.header),
        mainColIdx: oldDict.findColumn(mainColumn),
    )

proc reconcileChange*(self: var CsvDictSnapshotWithMainColumn): CsvDict =
    self.oldDict.mergeImpl(self.newDict, @[self.mainColIdx])
    return self.oldDict

func find*(self: CsvDictSnapshotWithMainColumn, key: string): Option[RowPos] =
    let rowPos = self.findRowImpl(@[(self.mainColIdx, key)])
    if rowPos.rowIdx == -1:
        return none(RowPos)
    return some(rowPos)

func `[]`*(self: CsvDictSnapshotWithMainColumn, key: string): seq[string] =
    let rowPos = self.findRowImpl(@[(self.mainColIdx, key)])
    if rowPos.dictIdx == 0:
        return self.oldDict[rowPos.rowIdx]
    return self.newDict[rowPos.rowIdx]

func `[]`*(self: CsvDictSnapshotWithMainColumn, key: string, column: string): string =
    # Kind of ugly, but effective
    self[key][self.oldDict.findColumn(column)]

proc `[]=`*(self: var CsvDictSnapshotWithMainColumn, key: string, newRow: seq[string]) =
    let rowPos = self.findRowImpl(@[(self.mainColIdx, key)])
    if rowPos.dictIdx == 0:
        self.newDict.add newRow
    else:
        self.newDict[rowPos.rowIdx] = newRow

proc update*(self: var CsvDictSnapshotWithMainColumn, key: string, values: openarray[ValueWithColName]) =
    self.update(self.findRowImpl(@[(self.mainColIdx, key)]), values)
