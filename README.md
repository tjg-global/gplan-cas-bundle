# gplan-cas-bundle

## Overview

Determine which SQL files fall into a range of commits and produce a single .sql file
containing the combined text of those files.

From a repo containing .sql files, each of which contains one or more changes to
a single database, build up a single release .sql file which can be applied to
that database. The release file is given a tag which is then held on the database.

The intention is that, run with only a database parameter, the package can determine
the last commit which has already been applied to that database. Assuming that everything
is to be applied up the current commit, the range of relevant commits is searched for
files whose path matches a pattern (by default, simply "*.sql").

Those files are combined in alphabetic order into a single release bundle which is
topped and tailed with code which will stamp the release bundle's metadata onto the
database, to be picked up next time as the latest commit used.

## Requirements:

* Fundamentally: end up with a single release file which can be version-controlled
* With minimum parameters, determine the combination of code for a single release
* Keep track of the latest release on the database itself so that the next release
  set can be determined automatically
* Filter files so unrelated files can be excluded

## Installation

Simplest is to clone (and/or fork) the repo:

    git clone https://github.com/tjg-global/gplan-cas-bundle

and then `pip install` in dev mode:

    python -mpip install -e <path-to-clone>

This will create a `gbundle` executable (.exe on Windows, shell script on *nix) which
can be used to run the program

## Running

`gbundle` uses [argparse](https://docs.python.org/3/library/argparse.html) so `gbundle --help` will produce useful output

### Examples

* Using a database named `TDI` on server `SVR-DB-CAS-DEV`, read the most recent
  release applied to that database for the `gplan-cas` project and find any further
  commits to the repository whose working directory is at `C:\work-in-progress\code`:

    `gbundle --dburi=SVR-DB-CAS-DEV/TDI --repo=C:\work-in-progress\code`

* Produce a release bundle from a specified list of files contained in `files.txt`
  against the current commit of the repository in `C:\work-in-progress\code`:

    `gbundle --files=files.txt --repo=c:\work-in-progress\code`

* Produce a release bundle to be applied to a database named `TDI` on server `SVR-DB-CAS-DEV`.
  The bundle should use the repository in `C:\work-in-progress\code` and consider all
  files changed between commit `2a7ff99ec` and the latest commit to `main`/`master`.
  Only consider files which match the pattern `*/tdi/sql/*.sql`

    `gbundle --dburi=SVR-DB-CAS-DEV/TDI --repo=C:\work-in-progress\code --from-commit=2a7ff99ec --code-pattern="*/tdi/sql/*.sql"`
