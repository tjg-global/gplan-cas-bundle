#!python3
"""Determine which SQL files fall into a range of commits and produce a single bundle

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
"""
import os, sys
import argparse
import atexit
import codecs
import fnmatch
import locale
import logging
import re
import subprocess
import tempfile
import time
import urllib.parse

import git
#
# On MacOS pyodbc is harder to find/build
#
try:
    import pyodbc
except ImportError:
    pyodbc = None

logger = logging.getLogger(__name__)

drivers = {
    "mssql" : "ODBC Driver 17 for SQL Server"
}

REPO_DIRPATH = "."
CODE_PATTERN = "*.sql"
RELEASES_RELPATH = "releases"
RELEASE_TYPE = "gplan-cas"

NEWLINE = "\n"
ENCODING = "utf-8"
ENCODING_COOKIE_RE = re.compile(
     "^[ \t\v]*#.*?coding[:=][ \t]*([-_.a-zA-Z0-9]+)"
)

def init_logging():
    """Ensure some basic logging
    """
    logger.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler()
    stdout_handler.setLevel(logging.DEBUG)
    logger.addHandler(stdout_handler)

def sniff_newline_convention(text):
    """Determine which line-ending convention predominates in the text.
    Windows usually has U+000D U+000A
    Posix usually has U+000A
    But editors can produce either convention from either platform. And
    a file which has been copied and edited around might even have both!
    """
    candidates = [
        ("\r\n", "\r\n"),
        # Match \n at the start of the string
        # or \n preceded by any character other than \r
        ("\n", "^\n|[^\r]\n"),
    ]
    #
    # If no lines are present, default to the platform newline
    # If there's a tie, use the platform default
    #
    conventions_found = [(0, 1, os.linesep)]
    for candidate, pattern in candidates:
        instances = re.findall(pattern, text)
        convention = (len(instances), candidate == os.linesep, candidate)
        conventions_found.append(convention)
    majority_convention = max(conventions_found)
    return majority_convention[-1]

def sniff_encoding(filepath):
    """Determine the encoding of a file:

    * If there is a BOM, return the appropriate encoding
    * If there is a PEP 263 encoding cookie, return the appropriate encoding
    * Otherwise return None for read_and_decode to attempt several defaults
    """
    boms = [
        (codecs.BOM_UTF8, "utf-8-sig"),
        (codecs.BOM_UTF16_BE, "utf-16"),
        (codecs.BOM_UTF16_LE, "utf-16"),
    ]
    #
    # Try for a BOM
    #
    with open(filepath, "rb") as f:
        line = f.readline()
    for bom, encoding in boms:
        if line.startswith(bom):
            return encoding
    #
    # Look for a PEP 263 encoding cookie
    #
    default_encoding = locale.getpreferredencoding()
    try:
        uline = line.decode(default_encoding)
    except UnicodeDecodeError:
        #
        # Can't even decode the line in order to match the cookie
        #
        pass
    else:
        match = ENCODING_COOKIE_RE.match(uline)
        if match:
            cookie_codec = match.group(1)
            try:
                codecs.lookup(cookie_codec)
            except LookupError:
                logger.warning(
                    "Encoding cookie has invalid codec name: {}".format(
                        cookie_codec
                    )
                )
            else:
                return cookie_codec
    #
    # Fall back to the locale default
    #
    return None

def read_and_decode(filepath):
    """
    Read the contents of a file, heuristically determining the encoding and
    newline convention
    """
    sniffed_encoding = sniff_encoding(filepath)
    #
    # If sniff_encoding has found enough clues to indicate an encoding,
    # use that. Otherwise try a series of defaults before giving up.
    #
    if sniffed_encoding:
        #~ logger.debug("Detected encoding %s", sniffed_encoding)
        candidate_encodings = [sniffed_encoding]
    else:
        candidate_encodings = [ENCODING, locale.getpreferredencoding()]

    with open(filepath, "rb") as f:
        btext = f.read()
    for encoding in candidate_encodings:
        #~ logger.debug("Trying to decode with %s", encoding)
        try:
            text = btext.decode(encoding)
            #~ logger.info("Decoded with %s", encoding)
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        raise UnicodeDecodeError(encoding, btext, 0, 0, "Unable to decode")

    #
    # Sniff and convert newlines here so that, by the time
    # the text reaches us it is ready to use. Then
    # convert everything to the preferred newline character
    #
    newline = sniff_newline_convention(text)
    #~ logger.debug("Detected newline %r", newline)
    text = re.sub("\r\n", NEWLINE, text)
    return text

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

    logger.info("Connect to database %s/%s", server, database)
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
    logger.info("Create a temporary clone of the repo from %s", repo_dirpath)
    main_branches = {"main", "master"}
    temp_dirpath = tempfile.mkdtemp()
    source_repo = git.Repo(repo_dirpath)
    temp_repo = source_repo.clone(temp_dirpath)
    origin = temp_repo.remote("origin")
    for branch in main_branches:
        if branch in origin.refs:
            main = origin.refs[branch]
            logger.debug("Using branch %s", main)
            break
    else:
        raise RuntimeError("Unable to find a main branch")
    temp_repo.create_head(branch, main).checkout()
    #~ atexit.register(remove_temporary_repo, temp_repo.working_dir)

    return temp_repo

def remove_temporary_repo(repo_dirpath):
    """Remove the temporary repo created earlier

    NB although the temporary repo is created in the %TEMP% filespace, it's
    known to be held open by things like virus checkers and the TortoiseGit
    cache process. So attempt a couple of retries before giving up
    """
    logger.info("Remove the temporary clone created at %s", repo_dirpath)
    n_try = 1
    while True:
        try:
            os.removedirs(repo_dirpath)
            return
        except OSError:
            if n_try <= 3:
                n_try += 1
                logger.warning("Couldn't remove temporary repo at %r; retrying after a delay", repo_dirpath)
                time.sleep(1)
            else:
                logger.exception("Unable to remove temporary repo at %r after %d tries", repo_dirpath, n_try)
                return

def checkout_to_specific_commit(repo, commit):
    """Check out the temporary repo to a specific commit to pick up the
    files requested at the right point in time
    """
    logger.info("Check out the repo %r to commit %s", repo, commit)
    repo.head.reference = commit
    repo.head.reset(index=True, working_tree=True)

def get_release_bundle_from_db(db, release_type=RELEASE_TYPE):
    """Determine the last release to be applied to this database
    """
    logger.info("Determine the last release of type %s applied to database %r", release_type, db)
    with db.cursor() as q:
        return q.execute("SELECT release.fn_release_bundle(?)", [release_type]).fetchval()

def get_short_sha(repo, sha, length=8):
    """Use git's built-in logic to find a shortened form of a commit SHA
    """
    logger.info("Find a length %s form of a commit SHA %s", length, sha)
    return repo.git.rev_parse(sha, short=length)

def get_latest_commit_sha_from_db(db):
    """Parse the latest release to find the last commit applied
    """
    logger.info("Parse the latest release applied to database %r to find the last commit applied", db)
    bundle_name = get_release_bundle_from_db(db)
    logger.debug("Found existing bundle name: %s", bundle_name)
    if bundle_name:
        try:
            tag, from_commit, to_commit = bundle_name.split("-")
            return to_commit
        except (ValueError, TypeError):
            logger.warning("Unable to extract a commit from the release name %s", bundle_name)
            return None
    else:
        return None

def get_earliest_commit_from_repo(repo):
    """Get the first commit on the current branch of the repo
    """
    logger.info("Find the first commit to repo %r", repo)
    for c in repo.iter_commits():
        pass
    return c

def get_latest_commit_from_repo(repo):
    """Get the latest commit on the current branch of the repo
    """
    logger.info("Find the latest commit to repo %r", repo)
    return repo.head.commit

def get_bundle_name(release_tag, from_commit, to_commit):
    """Generate a bundle name from the release tag & first/last commits
    """
    logger.info("Generate a bundle name from tag %s and commits %s/%s", release_tag, from_commit.hexsha, to_commit.hexsha)
    from_sha = get_short_sha(from_commit.repo, from_commit.hexsha)
    to_sha = get_short_sha(to_commit.repo, to_commit.hexsha)
    return "%s-%s-%s" % (release_tag, from_sha, to_sha)

def generate_prologue(release_type, bundle_name, database_name):
    if database_name: yield f"USE {database_name}\nGO\n"
    yield f"DECLARE @v_release_bundle VARCHAR(60) = '{bundle_name}';"
    yield f"DECLARE @v_current_bundle VARCHAR(60) = release.fn_release_bundle('{release_type}');"
    yield "IF @v_release_bundle < @v_current_bundle THROW 51000, 'The incoming release bundle is older than the last one applied. Use @i_override_newer to override', 1;\n"

def generate_epilogue(release_type, bundle_name):
    yield f"EXEC release.pr_tag_release_bundle @i_release_type = '{release_type}', @i_release_bundle = '{bundle_name}';"

def generate_separator():
    yield "GO\n"

def generate_file_contents(filepath):
    yield re.sub(r"(?:\n|^)USE\s+.*\nGO\s*\n", "", read_and_decode(filepath), flags=re.IGNORECASE)

def get_rel_filepaths_between_commits(repo, from_commit, to_commit):
    """Find affected filepaths relative to a repo root between two commits
    """
    logger.info("Find affected filepaths relative to %s between commits %s & %s", repo, from_commit, to_commit)
    rel_filepaths = set()
    for diff in from_commit.diff(to_commit):
        rel_filepaths.add(diff.a_path)
        rel_filepaths.add(diff.b_path)
    return rel_filepaths

def create_release_bundle(bundle_filepath, database_name, release_type, bundle_name, repo, rel_filepaths, code_pattern):
    logger.info("Generate a release file at %s from repo %r using files matching '%s'", bundle_filepath, repo, code_pattern)
    with open(bundle_filepath, "w") as f:
        #
        # Write the bundle prologue
        #
        f.write("\n".join(generate_prologue(release_type, bundle_name, database_name)) + "\n")
        f.write("\n".join(generate_separator()) + "\n")

        #
        # Add each relevant file
        #
        for relpath in sorted(rel_filepaths):
            filepath = os.path.abspath(os.path.join(repo.working_tree_dir, relpath))
            if fnmatch.fnmatch(relpath, code_pattern):
                if os.path.exists(filepath):
                    logger.info("USING %s", relpath)
                    f.write(f"--\n-- {relpath}\n--\n")
                    f.write("\n".join(generate_file_contents(filepath)) + "\n")
                    f.write("\n".join(generate_separator()) + "\n")
                else:
                    logger.warning("SKIPPING '%s': no longer in the filesystem", relpath)
            else:
                logger.warning("SKIPPING '%s': doesn't match code pattern '%s'", relpath, code_pattern)

        #
        # Write the bundle epilogue
        #
        f.write("n".join(generate_epilogue(release_type, bundle_name)) + "\n")
        f.write("\n".join(generate_separator()) + "\n")

def main(
    repo_dirpath,
    release_tag,
    dburi,
    from_commit=None,
    to_commit=None,
    files_filepath=None,
    code_pattern=CODE_PATTERN,
    releases_relpath=RELEASES_RELPATH,
    release_type=RELEASE_TYPE
):
    """Produce a release bundle from a repo destined for a target database and then
    tag the database with the bundle used

    By default, this will determine from the database the last commit released to
    that database, and will generate a release script incorporating every change
    between that commit and the current HEAD
    """
    db = database_name = None
    if dburi:
        _, _, database_name, _, _ = parse_dburi_ex(dburi)
        if pyodbc:
            db = database(dburi)
        else:
            logger.warning("No pyodbc module available to connect to %s" % dburi)

    #
    # Create a temporary clone of the repository at `repo_dirpath` so we
    # don't intefere with a working copy
    #
    repo = create_temporary_repo(repo_dirpath)

    if not release_tag:
        release_tag = time.strftime("%Y%m%d-%H%M%S")

    #
    # Attempt to determine a commit from:
    # i) The command line
    # ii) The database (if given)
    # iii) The earliest commit in the repo
    #
    if from_commit:
        from_commit = repo.commit(from_commit)
    if not from_commit and db:
        sha = get_latest_commit_sha_from_db(db)
        if sha:
            from_commit = repo.commit(sha)
    if not from_commit:
        from_commit = get_earliest_commit_from_repo(repo)

    #
    # Attempt to determine a commit from:
    # i) The command line
    # ii) The latest commit in the repo
    #
    if to_commit:
        to_commit = repo.commit(to_commit)
    if not to_commit:
        to_commit = get_latest_commit_from_repo(repo)
    if from_commit == to_commit:
        raise RuntimeError("No changes between latest & current commits")

    #
    # Move the head of the repo to the to-commit determined above
    # This is so that we have the version of each file at that commit
    #
    checkout_to_specific_commit(repo, to_commit)

    #
    # If a list of files is specified, use that; otherwise, determine
    # the files changed between the two commits
    #
    if files_filepath:
        with open(files_filepath) as f:
            rel_filepaths = [l.strip() for l in f]
    else:
        rel_filepaths = get_rel_filepaths_between_commits(repo, from_commit, to_commit)

    #
    # Determine where the release bundle should go and what it should be called
    #
    releases_dirpath = os.path.join(repo_dirpath, releases_relpath)
    if not os.path.isdir(releases_dirpath):
        raise RuntimeError("Release path %s does not exist or is not a directory" % releases_dirpath)
    bundle_name = get_bundle_name(release_tag, from_commit, to_commit)
    bundle_filepath = os.path.abspath(os.path.join(releases_dirpath, "%s.sql" % bundle_name))

    #
    # Create the release bundle
    #
    create_release_bundle(bundle_filepath, database_name, release_type, bundle_name, repo, rel_filepaths, code_pattern)

def command_line():
    parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
    parser.add_argument("--repo-dirpath", default=".", help="The root of a working copy of the repo. Default: the current directory" )
    parser.add_argument("--release-tag", help="A prefix for the release bundle name. Typically it will be a release version. Default: a generated timestamp")
    parser.add_argument("--dburi", help="A database URI in the form server/database optionally including a username and password eg tim:5ecret@svr-db1/tdi. Default: no database access is attempted")
    parser.add_argument("--from-commit", help="A SHA (or tag etc.) representing the first commit to be used. Default: the last commit will be picked up from the database if available; otherwise the first commit to master/main")
    parser.add_argument("--to-commit", help="A SHA (or tag etc.) representing the last commit to be used. Default: the last commit to master/main")
    parser.add_argument("--code-pattern", default=CODE_PATTERN, help="A unix-style file pattern relative to the repo root indicating which files are to be selected. Default: all .sql files")
    parser.add_argument("--files", help="A file containing a list of files paths relative to the repo root to be released in this bundle. Default: all files between the from & to commits which match the code pattern")
    parser.add_argument("--releases-relpath", default=RELEASES_RELPATH, help="A directory relative to the repo root where release bundles are to be created. Default: <repo>/releases")
    parser.add_argument("--release-type", default=RELEASE_TYPE, help="When holding the metadata for this release on the database, this release type is the key. Default: gplan-cas")
    args = parser.parse_args()
    logger.debug(args)

    main(
        args.repo_dirpath,
        args.release_tag,
        args.dburi,
        args.from_commit,
        args.to_commit,
        args.files,
        args.code_pattern,
        args.releases_relpath,
        args.release_type
    )

if __name__ == '__main__':
    init_logging()
    command_line()
