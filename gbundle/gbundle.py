#!python3
import os, sys
import argparse
import logging
import re
import subprocess
import tempfile
import urllib.parse

import git
import pyodbc

logger = logging.getLogger(__name__)

drivers = {
    "mssql" : "ODBC Driver 17 for SQL Server"
}

REPO_DIRPATH = "."
CODE_RELPATH = "code"
RELEASES_RELPATH = "releases"
OVERRIDE_NEWER = False
RELEASE_TYPE = "gplan-cas"

def init_logging():
    """Ensure some basic logging
    """
    logger.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)

def parse_dburi_ex(dburi):
    """Break out a URI-style database connection into its component parts

    The most common string will be simply <server>/<database> (eg SVR09/TDI)
    In addition, as username/password can be included following the URI
    convention, eg tim:secret@SVR09/TDI
    If needed, the scheme can indicate which database type to connect to,
    eg mssql://svr-db-cas-dev/TDI_DEV
    At present only SQL server is supported via "mssql://"
    """
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
    """Return a database connection represented by the URI-style connection string

    cf parse_dburi_ex above for examples of connection strings
    """
    connectors = {}
    scheme, server, database, username, password = parse_dburi_ex(dburi)
    if scheme.lower() != "mssql":
        raise RuntimeError("Only MSSQL connections allowed for now")
    connectors["driver"] = drivers[scheme]
    connectors["server"] = server
    connectors["database"] = database
    if username:
        connectors["uid"] = username
        connectors["pwd"] = password
    else:
        connectors["Trusted_Connection"] = "Yes"
    connection_string = ";".join("%s=%s" % c for c in connectors.items())
    return pyodbc.connect(connection_string, autocommit=True)

def create_temporary_repo(repo_dirpath):
    """Make a temporary copy of the relevant repo

    We do this because we don't want to get fouled up in staged or uncommitted
    changes, and we want to work off the master branch
    """
    main_branches = {"main", "master"}
    temp_dirpath = tempfile.mkdtemp()
    source_repo = git.Repo(repo_dirpath)
    temp_repo = source_repo.clone(temp_dirpath)
    origin = temp_repo.remote("origin")
    for branch in main_branches:
        if branch in origin.refs:
            main = origin.refs[branch]
            break
    else:
        raise RuntimeError("Unable to find a main branch")
    temp_repo.create_head(branch, main).checkout()
    return temp_repo

def checkout_to_specific_commit(repo, commit):
    repo.head.reference = commit
    repo.head.reset(index=True, working_tree=True)

def get_release_bundle_from_db(db, release_type=RELEASE_TYPE):
    with db.cursor() as q:
        return q.execute("SELECT release.fn_release_bundle(?)", [release_type]).fetchval()

def get_short_sha(repo, sha, length=8):
    return repo.git.rev_parse(sha, short=length)

def get_latest_commit_sha_from_db(db):
    bundle_name = get_release_bundle_from_db(db)
    if bundle_name:
        try:
            tag, from_commit, to_commit = bundle_name.split("-")
            return to_commit
        except (ValueError, TypeError):
            logger.warn("Unable to extract a commit from the release name %s", bundle_name)
            return None
    else:
        return None

def get_earliest_commit_from_repo(repo):
    for c in repo.iter_commits():
        pass
    return c

def get_latest_commit_from_repo(repo):
    return repo.head.commit

def generate_bundle_name(release_tag, from_commit, to_commit):
    from_sha = get_short_sha(from_commit.repo, from_commit.hexsha)
    to_sha = get_short_sha(to_commit.repo, to_commit.hexsha)
    return "%s-%s-%s" % (release_tag, from_sha, to_sha)

def generate_release_bundle(dirpath, repo, release_tag, from_commit, to_commit, filepaths):
    bundle_name = generate_bundle_name(release_tag, from_commit, to_commit)
    checkout_to_specific_commit(repo, to_commit)
    logger.debug("Filepaths: %s", filepaths)
    bundle_filepath = os.path.abspath("%s.sql" % bundle_name)
    with open(bundle_filepath, "w") as f:
        for relpath in filepaths:
            filepath = os.path.join(repo.working_tree_dir, "code", relpath) ## FIXME -- need to get code folder
            with open(filepath) as g:
                f.write("--\n-- %s\n--\n" % (relpath))
                f.write(g.read())
                f.write("\n\n")
    logger.debug("Bundle name: %s", bundle_name)
    logger.debug("Bundle file: %s", bundle_filepath)
    return bundle_name

def tag_release_bundle(db, bundle_name, release_type=RELEASE_TYPE, override_newer=OVERRIDE_NEWER):
    with db.cursor() as q:
        q.execute(
            "EXEC release.pr_tag_release_bundle @i_release_bundle = ?, @i_release_type = ?, @i_override_newer = ?",
            [bundle_name, release_type, override_newer]
        )

def get_rel_filepaths_between_commits(repo, from_commit, to_commit):
    raise NotImplementedError

def main(
    dburi,
    release_tag,
    repo_dirpath=REPO_DIRPATH,
    from_commit=None,
    to_commit=None,
    files_filepath=None,
    code_relpath=CODE_RELPATH,
    releases_relpath=RELEASES_RELPATH,
    release_type=RELEASE_TYPE,
    override_newer=OVERRIDE_NEWER
):
    """Produce a release bundle from a repo destined for a target database and then
    tag the database with the bundle used

    By default, this will determine from the database the last commit released to
    that database, and will generate a release script incorporating every change
    between that commit and the current HEAD
    """
    db = database(dburi)
    repo = create_temporary_repo(repo_dirpath)
    if not from_commit:
        sha = get_latest_commit_sha_from_db(db)
        if sha:
            from_commit = repo.commit(sha)
    if not from_commit:
        from_commit = get_earliest_commit_from_repo(repo)
    if not to_commit:
        to_commit = get_latest_commit_from_repo(repo)
    if files_filepath:
        with open(files_filepath) as f:
            filepaths = [l.strip() for l in f]
    else:
        rel_filepaths = get_rel_filepaths_between_commits(repo, from_commit, to_commit)
        filepaths = [os.path.join(repo_dirpath, code_relpath, l) for l in rel_filepaths]
    releases_dirpath = os.path.join(repo_dirpath, releases_relpath)
    logger.debug("repo: %s", repo)
    logger.debug("from: %s", from_commit)
    logger.debug("to: %s", to_commit)
    logger.debug("filepaths: %s", filepaths)

    bundle_name = generate_release_bundle(releases_dirpath, repo, release_tag, from_commit, to_commit, filepaths)
    #~ tag_release_bundle(db, bundle_name, release_type, override_newer)

def command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dburi", required=True)
    parser.add_argument("--release-tag", required=True)
    parser.add_argument("--repo-dirpath", default=".")
    parser.add_argument("--from-commit")
    parser.add_argument("--to-commit")
    parser.add_argument("--files")
    parser.add_argument("--code-relpath", default=CODE_RELPATH)
    parser.add_argument("--releases-relpath", default=RELEASES_RELPATH)
    parser.add_argument("--release-type", default=RELEASE_TYPE)
    parser.add_argument("--override-newer", default=OVERRIDE_NEWER)
    args = parser.parse_args()
    logger.debug(args)

    main(
        args.dburi,
        args.release_tag,
        args.repo_dirpath,
        args.from_commit,
        args.to_commit,
        args.files,
        args.code_relpath,
        args.releases_relpath,
        args.release_type,
        args.override_newer
    )

if __name__ == '__main__':
    init_logging()
    command_line()
