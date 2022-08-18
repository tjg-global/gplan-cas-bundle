from gbundle import gbundle
import git

db = gbundle.database("SVR-DB-CAS-DEV/TDI")
repo = git.Repo(".")