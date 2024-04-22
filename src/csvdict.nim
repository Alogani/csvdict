import std/[algorithm, options, parsecsv, streams]

type
    CsvDict* = object of RootObj
        header*: seq[string]
        rows: seq[seq[string]]

    ValueWithColName = (string, string)
    ValueWithColIdx = (int, string)
    Comparator = proc(a, b: string): int

# IO
proc readCsv*(input: Stream, separator = ';', skipInitialSpace = false): Option[CsvDict]
proc writeCsv*(csvDict: CsvDict, output: Stream, separator = ';', rowMinSizes: seq[int] = @[])
func rowToCsv(row: seq[string], separator = ';', rowMinSizes: seq[int]): string

proc init*(T: type CsvDict, header: seq[string]): CsvDict
# Seq like operations
proc add*(self: var CsvDict, row: seq[string])
proc add*(self: var CsvDict, row: openArray[ValueWithColName])
proc insert*(self: var CsvDict, row: seq[string], i: int)
proc insert*(self: var CsvDict, row: openArray[ValueWithColName], i: int)
proc del*(self: var CsvDict, keyDict: openarray[ValueWithColName])
proc delete*(self: var CsvDict, keyDict: openarray[ValueWithColName])
proc delAll*(self: var CsvDict, keyDict: openarray[ValueWithColName])
proc deleteAll*(self: var CsvDict, keyDict: openarray[ValueWithColName])
proc len*(self: CsvDict): int
proc sort*(self: var CsvDict, comparator: openArray[tuple[column: string, cmp: Comparator]], order = SortOrder.Ascending)
# Global operations
proc `&`*(a, b: CsvDict): CsvDict
proc merge*(dest: var CsvDict, src: CsvDict, keyColumns: seq[string])
proc mergeImpl(dest: var CsvDict, src: CsvDict, keyColumnsIdx: seq[int]) {.inline.}
# Table like operations
func find*(self: CsvDict, keyDict: openArray[ValueWithColName]): int
func `[]`*(self: CsvDict, i: int): seq[string]
func `[]`*(self: CsvDict, keyDict: openarray[ValueWithColName]): seq[string]
func `[]`*(self: CsvDict, keyDict: openarray[ValueWithColName], column: string): string
proc `[]=`*(self: var CsvDict, i: int, column: string, value: string)
proc `[]=`*(self: var CsvDict, i: int, newRow: seq[string])
proc update*(self: var CsvDict, i: int, values: openarray[ValueWithColName])
proc `[]=`*(self: var CsvDict, keyDict: openarray[ValueWithColName], column: string, value: string)
proc `[]=`*(self: var CsvDict, keyDict: openarray[ValueWithColName], newRow: seq[string])
proc update*(self: var CsvDict, keyDict: openarray[ValueWithColName], values: openarray[ValueWithColName])
func getAll*(self: CsvDict, keyDict: openarray[ValueWithColName], column: string): seq[string]
func getAll*(self: CsvDict, keyDict: openarray[ValueWithColName]): seq[seq[string]]
proc setAll*(self: var CsvDict, keyDict: openarray[ValueWithColName], values: openarray[ValueWithColName])
# Misc
func findRows*(self: CsvDict, keyDict: openarray[ValueWithColName], stopAtFirstMatch: bool): seq[int]
func findRowsImpl(self: CsvDict, keyIdx: seq[ValueWithColIdx], stopAtFirstMatch: bool): seq[int] {.inline.}
func findColumn*(self: CsvDict, column: string): int {.inline.}
func convertColNameToIdx(self: CsvDict, idxDict: seq[(ValueWithColName)]): seq[ValueWithColIdx]
proc updateRow(row: var seq[string], idxPair: seq[ValueWithColIdx])
func toRow(self: CsvDict, idxDict: seq[ValueWithColName]): seq[string]
func match(keyIdx: seq[ValueWithColIdx], row: seq[string]): bool



# IO
proc readCsv*(input: Stream, separator = ';', skipInitialSpace = false): Option[CsvDict] =
    var parser: CsvParser
    open(parser, input, filename = "", separator = separator, skipInitialSpace = skipInitialSpace)
    while true:
        if not parser.readRow():
            return none(CsvDict)
        if parser.row.len() != 0 and parser.row[0] != "":
            break
    var csvDict = CsvDict.init(parser.row)
    while parser.readRow():
        if parser.row.len() != 0 and parser.row[0] != "":
            csvDict.rows.add parser.row
    return some(csvDict)

proc writeCsv*(csvDict: CsvDict, output: Stream, separator = ';', rowMinSizes: seq[int] = @[]) =
    ## rowMinSizes is for human readability
    var rowMinSizesFilled = rowMinSizes
    for i in high(csvDict.header) ..< high(rowMinSizes):
        rowMinSizesFilled.add 0
    output.writeLine(rowToCsv(csvDict.header, separator, rowMinSizesFilled))
    for row in csvDict.rows:
        if row.len() != 0:
            output.writeLine(rowToCsv(row, separator, rowMinSizesFilled))

func rowToCsv(row: seq[string], separator = ';', rowMinSizes: seq[int]): string =
    assert row.len() == rowMinSizes.len()
    var
        allStrIndex = newSeq[int](rowMinSizes.len())
        totalResultLen: int
    for i in 0..high(row):
        allStrIndex[i] = totalResultLen
        let fieldLen =
            if i != high(row):
                max(row[i].len() + 1, rowMinSizes[i])
            else:
                row[i].len() + 1
        totalResultLen += fieldLen
    result = newString(totalResultLen)
    for i in 0..<totalResultLen: result[i] = ' '
    for i in 0..high(row):
        result[allStrIndex[i] ..< allStrIndex[i] + row[i].len()] = row[i]
        result[allStrIndex[i] + row[i].len()] = separator

proc init*(T: type CsvDict, header: seq[string]): CsvDict =
    CsvDict(header: header)

# Seq like operations
proc add*(self: var CsvDict, row: seq[string]) =
    self.rows.add row

proc add*(self: var CsvDict, row: openArray[ValueWithColName]) =
    self.rows.add self.toRow(@row)

proc insert*(self: var CsvDict, row: seq[string], i: int) =
    self.rows.insert(row, i)

proc insert*(self: var CsvDict, row: openArray[ValueWithColName], i: int) =
    self.rows.insert(self.toRow(@row), i)

proc del*(self: var CsvDict, keyDict: openarray[ValueWithColName]) =
    ## Don't preserve order, complexity O(1)
    self.rows.del(self.find(keyDict))

proc delete*(self: var CsvDict, keyDict: openarray[ValueWithColName]) =
    ## Preserve order, complexity O(n)
    self.rows.delete(self.find(keyDict))

proc delAll*(self: var CsvDict, keyDict: openarray[ValueWithColName]) =
    ## Don't preserve order, complexity O(1)
    for rowIdx in self.findRows(keyDict, false):
        self.rows.del(rowIdx)

proc deleteAll*(self: var CsvDict, keyDict: openarray[ValueWithColName]) =
    ## Preserve order, complexity O(n)
    for rowIdx in self.findRows(keyDict, false):
        self.rows.delete(rowIdx)

proc len*(self: CsvDict): int =
    self.rows.len()

proc sort*(self: var CsvDict, comparator: openArray[tuple[column: string, cmp: Comparator]], order = SortOrder.Ascending) =
    ## Order is important, put primary key first
    var comparatorWithIdx: seq[(int, Comparator)]
    for (key, cmpFn) in comparator:
        comparatorWithIdx.add (self.findColumn(key), cmpFn)
    self.rows.sort(proc(rowA, rowB: seq[string]): int =
        for (idx, cmpFn) in comparatorWithIdx:
            let sortResult = cmpFn(rowA[idx], rowB[idx])
            if sortResult != 0:
                return sortResult
    , order)

# Global operations
proc `&`*(a, b: CsvDict): CsvDict =
    if a.header != b.header: raise newException(ValueError, "header incompatible")
    result = CsvDict.init(a.header)
    result.rows = a.rows & b.rows

proc merge*(dest: var CsvDict, src: CsvDict, keyColumns: seq[string]) =
    if src.header != dest.header: raise newException(ValueError, "header incompatible")
    var keyColumnsIdx = newSeqOfCap[int](keyColumns.len())
    for column in keyColumns:
        keyColumnsIdx.add src.findColumn(column)
    dest.mergeImpl(src, keyColumnsIdx)
    
proc mergeImpl(dest: var CsvDict, src: CsvDict, keyColumnsIdx: seq[int]) =
    for row in src.rows:
        var keyIdx = newSeqOfCap[ValueWithColIdx](keyColumnsIdx.len())
        for idx in keyColumnsIdx:
            keyIdx.add (idx, row[idx])
        let foundIdx = dest.findRowsImpl(keyIdx, true)
        if foundIdx.len() == 0:
            dest.rows.add row
        else:
            dest.rows[foundIdx[0]] = row

# Table like operations
func find*(self: CsvDict, keyDict: openArray[ValueWithColName]): int =
    let rowIdx = self.findRows(keyDict, true)
    if rowIdx.len() == 0:
        return -1
    return rowIdx[0]

func `[]`*(self: CsvDict, i: int): seq[string] =
    self.rows[i]

func `[]`*(self: CsvDict, keyDict: openarray[ValueWithColName]): seq[string] =
    return self.rows[self.find(keyDict)]

func `[]`*(self: CsvDict, keyDict: openarray[ValueWithColName], column: string): string =
    return self.rows[self.find(keyDict)][self.findColumn(column)]

proc `[]=`*(self: var CsvDict, i: int, column: string, value: string) =
    self.rows[i].updateRow(self.convertColNameToIdx(@{column: value}))

proc `[]=`*(self: var CsvDict, i: int, newRow: seq[string]) =
    self.rows[i] = newRow

proc update*(self: var CsvDict, i: int, values: openarray[ValueWithColName]) =
    self.rows[i].updateRow(self.convertColNameToIdx(@values))

proc `[]=`*(self: var CsvDict, keyDict: openarray[ValueWithColName], column: string, value: string) =
    self.rows[self.find(keyDict)].updateRow(self.convertColNameToIdx(@{column: value}))

proc `[]=`*(self: var CsvDict, keyDict: openarray[ValueWithColName], newRow: seq[string]) =
    let idx = self.find(keyDict)
    if idx == -1:
         self.rows.add newRow
    else:
        self.rows[idx] = newRow

proc update*(self: var CsvDict, keyDict: openarray[ValueWithColName], values: openarray[ValueWithColName]) =
    self.rows[self.find(keyDict)].updateRow(self.convertColNameToIdx(@values))

func getAll*(self: CsvDict, keyDict: openarray[ValueWithColName], column: string): seq[string] =
    let columnIdx = self.findColumn(column)
    for rowIdx in self.findRows(keyDict, false):
        result.add self.rows[rowIdx][columnIdx]

func getAll*(self: CsvDict, keyDict: openarray[ValueWithColName]): seq[seq[string]] =
    for rowIdx in self.findRows(keyDict, false):
        result.add self.rows[rowIdx]

proc setAll*(self: var CsvDict, keyDict: openarray[ValueWithColName], values: openarray[ValueWithColName]) =
    let valueIdxPair = self.convertColNameToIdx(@values)
    for rowIdx in self.findRows(keyDict, false):
        self.rows[rowIdx].updateRow(valueIdxPair)

# Misc
func findRows(self: CsvDict, keyDict: openarray[ValueWithColName], stopAtFirstMatch: bool): seq[int] =
    let keyIdx = self.convertColNameToIdx(@keyDict)
    return self.findRowsImpl(keyIdx, stopAtFirstMatch)

func findRowsImpl(self: CsvDict, keyIdx: seq[ValueWithColIdx], stopAtFirstMatch: bool): seq[int] =
    for (idx, row) in self.rows.pairs():
        if keyIdx.match(row):
            result.add idx
            if stopAtFirstMatch:
                break

func findColumn(self: CsvDict, column: string): int =
    result = self.header.find(column)
    if result == -1:
        raise newException(ValueError, "Unknown key")

func convertColNameToIdx(self: CsvDict, idxDict: seq[ValueWithColName]): seq[ValueWithColIdx] =
    for (key, value) in idxDict:
        result.add (self.findColumn(key), value)
    result.sort(proc(a, b: ValueWithColIdx): int = cmp(a[0], b[0]))

func toRow(self: CsvDict, idxDict: seq[ValueWithColName]): seq[string] =
    var lastIdx = 0
    for (idx, value) in self.convertColNameToIdx(idxDict):
        if idx != lastIdx:
            raise newException(ValueError, "Incomplete row")
        result.add value
        lastIdx.inc()

proc updateRow(row: var seq[string], idxPair: seq[ValueWithColIdx]) =
    for (idx, val) in idxPair:
        row[idx] = val

func match(keyIdx: seq[ValueWithColIdx], row: seq[string]): bool =
    for (idx, value) in keyIdx:
        if row[idx] != value:
            return false
    return true


iterator rows*(self: CsvDict): seq[string] =
    for row in self.rows:
        yield row

iterator mrows*(self: var CsvDict): var seq[string] =
    for row in self.rows.mitems():
        yield row