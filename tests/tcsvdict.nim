import std/unittest

import csvdict


test "setters/getters":
    var csvDict = CsvDict.init(@["User", "Task", "Status", "Time"])
    check csvDict.header == @["User", "Task", "Status", "Time"]
    csvDict.add @["admin", "Backup", "Success", "2023-05-07"]
    csvDict.add @["collab", "Update", "Failure", "1970-01-01"]
    csvDict.add @["collab", "Backup", "Failure", "2020-01-01"]

    check csvDict[{"Task": "Backup"}, "Time"] == "2023-05-07"
    csvDict[{"Task": "Update"}, "Status"] = "Success"
    check csvDict[{"User": "collab", "Task": "Update"}, "Status"] == "Success"

    check csvDict[{"Task": "Backup"}] == @["admin", "Backup", "Success", "2023-05-07"]
    csvDict[{"User": "collab", "Task": "Backup"}] = @["collab", "Backup", "Success", "2023-05-07"]
    csvDict.update({"User": "collab", "Task": "Backup"}, {"User": "collab"})

    check csvDict.getAll({"User": "collab"}, "Task") == @["Update", "Backup"]
    csvDict.setAll({"User": "collab"}, {"Status": "Success"})

    check csvDict.getAll({"Task": "Update"}) == @[@["collab", "Update", "Success", "1970-01-01"]]
