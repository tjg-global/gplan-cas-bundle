#!python3
import os, sys
import argparse
import re
import subprocess
import urllib.parse

import git
import pyodbc

drivers = {
    "mssql" : "ODBC Driver 17 for SQL Server"
}

def parse_dburi_ex(dburi):
    if not re.match("[^:]+://", dburi):
        dburi = "mssql://" + dburi
    parsed = urllib.parse.urlparse(dburi)
    return (
        parsed.scheme or "mssql",
        parsed.hostname or "",
        (parsed.path or "").lstrip("/"),
        parsed.username or "",
        parsed.password or "",
    )

def database(dburi):
    connectors = {}
    scheme, server, database, username, password = parse_dburi_ex(dburi)
    connectors["driver"] = drivers[scheme]
    connectors["server"] = server
    connectors["database"] = database
    if username:
        connectors["uid"] = username
        connectors["pwd"] = password
    else:
        connectors["Trusted_Connection"] = "Yes"
    connection_string = ";".join("%s=%s" % c for c in connectors.items())
    print("Connection string:", connection_string)
    return pyodbc.connect(connection_string)

def main(dirpath, dburi, from_commit, to_commit):
    print("dirpath:", dirpath)
    print("dburi:", dburi)
    print("from:", from_commit)
    print("to:", to_commit)

def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dirpath")
    parser.add_argument("--dburi")
    parser.add_argument("--from-commit")
    parser.add_argument("--to-commit")
    args = parser.parse_args()
    main(args.dirpath, args.dburi, args.from_commit, args.to_commit)

if __name__ == '__main__':
    command_line()
