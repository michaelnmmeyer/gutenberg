#!/usr/bin/env python3

# Copyright (c) 2016-17, Michaël Meyer
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its contributors
#    may be used to endorse or promote products derived from this software without
#    specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Boilerplate removal code is borrowed from:
# https://github.com/c-w/Gutenberg/blob/master/gutenberg/cleanup/strip_headers.py
# Which itself borrows from:
# http://www14.in.tum.de/spp1307/src/strip_headers.cpp
#
# Copyright 2014 Clemens Wolff
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os, sys, re, sqlite3, tarfile, json
import random, urllib, unicodedata, zlib, time, datetime, http.client
from urllib.request import urlopen
from xml.etree import ElementTree
from multiprocessing import Pool
from string import ascii_uppercase
from email.utils import parsedate

# Default database path.
DB_PATH = "~/.gutenberg"

# Default number of worker processes for parallel downloads.
DOWNLOAD_POOL_SIZE = 4

# Where to find the Gutenberg catalog. Must be the address of the bz2 file, not
# the zip file.
CATALOG_URL = "http://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2"

# Where to find the list of Gutenberg mirrors.
MIRRORS_URL = "https://www.gutenberg.org/MIRRORS.ALL"

# https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
import unicodedata
import re

def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')

# List of Gutenberg HTTP mirrors.
def gutenberg_mirrors():
   tbl = getattr(gutenberg_mirrors, "tbl", None)
   if tbl is None:
      with urlopen(MIRRORS_URL) as fp:
         urls = re.findall(r"http://[^ \r\n]+", fp.read().decode("UTF-8"))
         tbl = [url.rstrip("/") for url in urls]
         try:
            # Download limits on this one.
            tbl.remove('http://www.gutenberg.org/dirs')
         except ValueError:
            pass
      gutenberg_mirrors.tbl = tbl
   return tbl

SCHEMA = """\
/* Informations about the state of the database.
 * Possible keys are:
 * - last_catalog_update: last day the Gutenberg catalog was updated. If not
 *   present, the catalog will be updated at startup. The catalog can be
 *   updated with the "update" command.
 */
CREATE TABLE IF NOT EXISTS Infos(
   key TEXT PRIMARY KEY UNIQUE NOT NULL,
   value TEXT NOT NULL
) WITHOUT ROWID;

/* List of issued download queries.
 * - last_issued: last time the query was issued, not necessarily the last time
 *   all ebooks matching the query have been downloaded.
 */
CREATE TABLE IF NOT EXISTS DownloadQueries(
   query TEXT PRIMARY KEY UNIQUE NOT NULL,
   last_issued DATETIME NOT NULL
) WITHOUT ROWID;

/* Ebooks metadata.
 * This is constructed from the Gutenberg catalog. Ebooks that are not available
 * as plain text are discarded. Columns meaning:
 * - key: the ebook identifier.
 * - metadata: metadata extracted from the Gutenberg catalog. This is a JSON
 *   document. It contains the following fields:
 *   - key: the ebook identifier (an unsigned integer).
 *   - author: list of authors.
 *   - title: book title, as a list of strings. There is one string per title
 *     line. When a book title spans multiple lines, it is often the case that
 *     the title proper is on the first line, and subtitles follow.
 *   - language: list of languages.
 *   - subject: list of subjects.
 *   All strings are encoded to UTF-8 and normalized to NFC.
 * - name: name of the file to download. Download URLs are generated
 *   dynamically. File names are of two forms:
 *   - 11716.txt, 11716-8.txt, 11716-0.txt, etc.
 *   - etext96/zncli10.txt
 * - encoding: encoding of the above file.
 * - last_modified: last modification of the above file, as reported by the
 *   Gutenberg catalog. Doesn't necessarily correspond to the real last
 *   modification date of the file due to sloppy editing of the Gutenberg
 *   catalog.
 */
CREATE TABLE IF NOT EXISTS Metadata(
   key INTEGER PRIMARY KEY UNIQUE NOT NULL,
   metadata TEXT NOT NULL,
   name TEXT UNIQUE NOT NULL,
   encoding TEXT NOT NULL,
   last_modified DATETIME NOT NULL
) WITHOUT ROWID;

/* Full-text index, for searching the contents of the metadata table.
 * Before indexing, values associated to a field are normalized to NFKC. Unicode
 * case folding is applied on the resulting strings. In addition, the ligatures
 * "œ" and "æ" are converted to ASCII equivalents, and all Unicode whitespace
 * characters are replaced with SPACE (U+0020).
 * This normalization process must be reproduced on query tokens for manually
 * searching the index.
 */
CREATE VIRTUAL TABLE IF NOT EXISTS Search USING fts3(
   key INTEGER PRIMARY KEY UNIQUE NOT NULL,
   language TEXT,
   author TEXT,
   title TEXT,
   subject TEXT,
   tokenize=simple
);

/* Ebooks contents.
 * - key: the ebook identifier.
 * - contents: ebook text, encoded to UTF-8, normalized to NFC, compressed with
 *   zlib. Boilerplate legalese is stripped.
 * - url: where the ebook was downloaded.
 * - last_modified: date of last modification. This is the date reported by the
 *   Gutenberg catalog, not the one reported by the server from which the file
 *   was downloaded.
 * - when_downloaded: when the ebook was downloaded.
 */
CREATE TABLE IF NOT EXISTS Data(
   key INTEGER PRIMARY KEY UNIQUE NOT NULL,
   contents BLOB NOT NULL,
   url TEXT UNIQUE NOT NULL,
   last_modified DATETIME NOT NULL,
   when_downloaded DATETIME NOT NULL
) WITHOUT ROWID;
"""

# We drop boilerplate text before inserting ebooks in the database. This has the
# disadvantage that every book has to be downloaded again if the boilerplate
# removal function is changed. On the other hand, preserving the original text
# forces external programs to reimplement a cleanup function. We could save
# both the original text and a normalized version, but this would take too much
# space. Alternatively, we could save a delta of the two versions in the
# database, but there is no python builtin function to do that. Besides,
# downloading files again might not be much slower (if at all) and is easier.

TEXT_START_MARKERS = {
   "*END*THE SMALL PRINT",
   "*** START OF THE PROJECT GUTENBERG",
   "*** START OF THIS PROJECT GUTENBERG",
   "This etext was prepared by",
   "E-text prepared by",
   "Produced by",
   "Distributed Proofreading Team",
   "Proofreading Team at http://www.pgdp.net",
   "http://gallica.bnf.fr)",
   "      http://archive.org/details/",
   "http://www.pgdp.net",
   "by The Internet Archive)",
   "by The Internet Archive/Canadian Libraries",
   "by The Internet Archive/American Libraries",
   "public domain material from the Internet Archive",
   "Internet Archive)",
   "Internet Archive/Canadian Libraries",
   "Internet Archive/American Libraries",
   "material from the Google Print project",
   "*END THE SMALL PRINT",
   "***START OF THE PROJECT GUTENBERG",
   "This etext was produced by",
   "*** START OF THE COPYRIGHTED",
   "The Project Gutenberg",
   "http://gutenberg.spiegel.de/ erreichbar.",
   "Project Runeberg publishes",
   "Beginning of this Project Gutenberg",
   "Project Gutenberg Online Distributed",
   "Gutenberg Online Distributed",
   "the Project Gutenberg Online Distributed",
   "Project Gutenberg TEI",
   "This eBook was prepared by",
   "http://gutenberg2000.de erreichbar.",
   "This Etext was prepared by",
   "This Project Gutenberg Etext was prepared by",
   "Gutenberg Distributed Proofreaders",
   "Project Gutenberg Distributed Proofreaders",
   "the Project Gutenberg Online Distributed Proofreading Team",
   "**The Project Gutenberg",
   "*SMALL PRINT!",
   "More information about this book is at the top of this file.",
   "tells you about restrictions in how the file may be used.",
   "l'authorization à les utilizer pour preparer ce texte.",
   "of the etext through OCR.",
   "*****These eBooks Were Prepared By Thousands of Volunteers!*****",
   "We need your donations more than ever!",
   " *** START OF THIS PROJECT GUTENBERG",
   "****     SMALL PRINT!",
   '["Small Print" V.',
   '      (http://www.ibiblio.org/gutenberg/',
   'and the Project Gutenberg Online Distributed Proofreading Team',
   'Mary Meehan, and the Project Gutenberg Online Distributed Proofreading',
   '                this Project Gutenberg edition.',
}

TEXT_END_MARKERS = {
   "*** END OF THE PROJECT GUTENBERG",
   "*** END OF THIS PROJECT GUTENBERG",
   "***END OF THE PROJECT GUTENBERG",
   "End of the Project Gutenberg",
   "End of The Project Gutenberg",
   "Ende dieses Project Gutenberg",
   "by Project Gutenberg",
   "End of Project Gutenberg",
   "End of this Project Gutenberg",
   "Ende dieses Projekt Gutenberg",
   "        ***END OF THE PROJECT GUTENBERG",
   "*** END OF THE COPYRIGHTED",
   "End of this is COPYRIGHTED",
   "Ende dieses Etextes ",
   "Ende dieses Project Gutenber",
   "Ende diese Project Gutenberg",
   "**This is a COPYRIGHTED Project Gutenberg Etext, Details Above**",
   "Fin de Project Gutenberg",
   "The Project Gutenberg Etext of ",
   "Ce document fut presente en lecture",
   "Ce document fut présenté en lecture",
   "More information about this book is at the top of this file.",
   "We need your donations more than ever!",
   "END OF PROJECT GUTENBERG",
   " End of the Project Gutenberg",
   " *** END OF THIS PROJECT GUTENBERG",
}

LEGALESE_START_MARKERS = {
   "<<THIS ELECTRONIC VERSION OF",
}

LEGALESE_END_MARKERS = {
   "SERVICE THAT CHARGES FOR DOWNLOAD",
}

# Fixed mess with os.linesep(). We only use LF.
def remove_boilerplate(text):
   """Remove lines that are part of the Project Gutenberg header or footer.
   Note: this function is a port of the C++ utility by Johannes Krugel. The
   original version of the code can be found at:
   http://www14.in.tum.de/spp1307/src/strip_headers.cpp
   Args:
      text (unicode): The body of the text to clean up.
   Returns:
      unicode: The text with any non-text content removed.
   """
   lines = text.splitlines()

   out = []
   i = 0
   footer_found = False
   ignore_section = False

   for line in lines:
      reset = False

      if i <= 600:
         # Check if the header ends here
         if any(line.startswith(token) for token in TEXT_START_MARKERS):
            reset = True

         # If it's the end of the header, delete the output produced so far.
         # May be done several times, if multiple lines occur indicating the
         # end of the header
         if reset:
            out = []
            continue

      if i >= 100:
         # Check if the footer begins here
         if any(line.startswith(token) for token in TEXT_END_MARKERS):
            footer_found = True

         # If it's the beginning of the footer, stop output
         if footer_found:
            break

      if any(line.startswith(token) for token in LEGALESE_START_MARKERS):
         ignore_section = True
         continue
      elif any(line.startswith(token) for token in LEGALESE_END_MARKERS):
         ignore_section = False
         continue

      if not ignore_section:
         out.append(line)
         i += 1

   return "\n".join(out).strip() + "\n"

def cleanup(text):
   # Strip the leading BOM (if there is one).
   if text.startswith('\uFEFF'):
      text = text[1:]
   # NFC Normalization
   text = unicodedata.normalize("NFC", text)
   # Uniformize line breaks
   text = "\n".join(text.splitlines())
   return text.strip()

# Were the ElementTree API not broken, we wouldn't have to hardcode this, nor
# to wrap all basic functions.
NAMESPACES = {
  "dcam": "http://purl.org/dc/dcam/",
  "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
  "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "dcterms": "http://purl.org/dc/terms/",
  "pgterms": "http://www.gutenberg.org/2009/pgterms/",
  "cc": "http://web.resource.org/cc/",
}

def find_node(root, expr):
   nodes = root.findall(expr, NAMESPACES)
   assert len(nodes) == 1
   return nodes[0]

def find_nodes(root, expr):
   return root.findall(expr, NAMESPACES)

def find_attrib(node, expr):
   namespace, name = expr.split(":")
   expr = "{%s}%s" % (NAMESPACES[namespace], name)
   for key, value in node.items():
      if key == expr:
         return value
   assert 0

def extract_author(ebook, key):
   authors = []
   nodes = find_nodes(ebook, "dcterms:creator/pgterms:agent/pgterms:name")
   for node in nodes:
      author = node.text
      # Jervey, Susan R. (Susan Ravenel) -> Jervey, Susan Ravenel
      match = re.match(r"([^,]+),[^(]+\(([^)]+)\)", author)
      if match:
         name = match.group(1)
         rest = match.group(2)
         author = "%s, %s" % (name, rest)
      authors.append(author)
   return authors

def extract_title(ebook, key):
   # There is one file with two titles (nearly the same).
   nodes = find_nodes(ebook, "dcterms:title")
   if nodes:
      return nodes[0].text.splitlines()
   return []

def extract_language(ebook, key):
   nodes = find_nodes(ebook, "dcterms:language/rdf:Description/rdf:value")
   return [node.text for node in nodes]

def extract_subject(ebook, key):
   nodes = find_nodes(ebook, "dcterms:subject/rdf:Description/rdf:value")
   return [node.text for node in nodes]

# There are a few empty files (e.g. 0 and 1070). There are also files that are
# not available as plain text.
def find_versions(ebook, key):
   files = {}
   for file in find_nodes(ebook, "dcterms:hasFormat/pgterms:file"):
      formats = find_nodes(file, "dcterms:format/rdf:Description/rdf:value")
      # ZIP files.
      if len(formats) != 1:
         continue
      mime = formats[0].text.strip()
      # Only care about plain text ebooks.
      if not mime.startswith("text/plain"):
         continue
      url = find_attrib(file, "rdf:about").strip()
      # Checking the mime type is not enough, there is a TIF file wrongly
      # reported as being plain text. Also, *.txt.utf-8 files are automatically
      # generated from other sources and can only be downloaded from the main
      # Gutenberg site, which has download limits; their modification date is
      # not reliable.
      ext = os.path.splitext(url)[1].lower()
      if ext != ".txt":
         continue

      # The catalog is rife with invalid links and typos:
      #   http://www.gutenberg.org/files/11052/11502-8.txt
      #   http://www.gutenberg.org/1/1/7/5/6/11756-0.txt
      # There are also useless files:
      #   http://www.gutenberg.org/files/31423/test.txt
      # Interesting files are of the form:
      #   http://www.gutenberg.org/files/11716/11716-8.txt
      #   http://www.gutenberg.org/dirs/etext96/zncli10.txt
      # We add a few exceptions to the {key}(-{digit})? format. This doesn't
      # cover all possible cases.
      dir, name = url.rsplit("/")[-2:]
      if dir.startswith("etext"):
         pass
      elif re.match(r"^%d(-(\d|txt|u|body|utf-16|utf-8))?.txt$" % key, name):
         pass
      else:
         continue

      # Encoding
      encoding = re.match("text/plain; charset=(.+)", mime)
      encoding = encoding and encoding.group(1) or "utf-8"
      # Last modification date, in the format of sqlite's datetime().
      last_mod = find_node(file, "dcterms:modified").text
      parts = tuple(int(n) for n in re.findall(r"\d+", last_mod))
      last_mod = datetime.datetime(*parts).strftime("%Y-%m-%d %H:%M:%S")
      # In case there are several versions of the same file.
      _, prev_mod = files.get(url, ("", ""))
      if last_mod > prev_mod:
         files[url] = (encoding, last_mod)

   return [(url, vals[0], vals[1]) for url, vals in files.items()]

def extract_download_infos(ebook, key):
   files = find_versions(ebook, key)
   if not files:
      return None, None, None

   # Most recent files first.
   files.sort(key=lambda x: x[2], reverse=True)

   # When the same file is available in ASCII and LATIN1, etc., the ASCII
   # version is lossy, so we prefer other versions. Likewise, we prefer UTF-8
   # to LATIN-1 and others.
   encs = {}
   for url, enc, last_mod in files:
      encs.setdefault(enc, []).append((url, last_mod))
   if len(encs) > 1 and "us-ascii" in encs:
      del encs["us-ascii"]
   if "utf-8" in encs:
      enc = "utf-8"
      url, last_mod = encs["utf-8"][0]
   else:
      enc, vals = encs.popitem()
      url, last_mod = vals[0]
   
   # Keep the constant part of the final URL:
   #   http://www.gutenberg.org/files/11716/11716-8.txt -> 11716-8.txt
   #   http://www.gutenberg.org/dirs/etext96/zncli10.txt -> etext96/zncli10.txt
   parts = url.rsplit("/", 2)
   if parts[-2].startswith("etext"):
      name = "/".join(parts[-2:])
   else:
      name = parts[-1]
   return name, enc, last_mod

EXTRACTORS = {
   "author": extract_author,
   "title": extract_title,
   "language": extract_language,
   "subject": extract_subject,
}

def parse_xml(fp, key):
   tree = ElementTree.parse(fp)
   ebook = find_node(tree, "pgterms:ebook")
   name, enc, last_mod = extract_download_infos(ebook, key)
   if name is None:
      return None, None, None, None
   fields = {}
   for field, func in EXTRACTORS.items():
      ret = func(ebook, key)
      if isinstance(ret, str):
         ret = cleanup(ret)
      elif ret is not None:
         ret = [cleanup(c) for c in ret]
      fields[field] = ret
   return name, enc, last_mod, fields

if __name__ == "__main__":
   PROGNAME = os.path.basename(sys.argv[0])

   def inform(msg):
      print("%s: %s" % (PROGNAME, msg), file=sys.stderr)
      sys.stderr.flush()

   def progress(nr, total, num_workers):
      sys.stderr.write("%s: downloading %d/%d files (%d workers)\r" % (PROGNAME, nr, total, num_workers))
      sys.stderr.flush()
   
   def progress_finish():
      sys.stderr.write("\n")

   def die(msg=None):
      if msg is not None:
         inform(msg)
      sys.exit(1)
else:
   def inform(msg):
      pass

   def progress(*args):
      pass
   
   def progress_finish():
      pass

   class GutenbergError(Exception):
      pass

   def die(msg=None):
      raise GutenbergError(msg is not None and msg or "unknown error")

# 3.txt -> https://www.ibiblio.org/pub/docs/books/gutenberg/0/3/3.txt
# 832.txt -> https://www.ibiblio.org/pub/docs/books/gutenberg/8/3/832/832.txt
# etext96/zncli10.txt -> https://www.ibiblio.org/pub/docs/books/gutenberg/etext96/zncli10.txt
def make_book_url(key, name):
   mirror = random.choice(gutenberg_mirrors())
   if name.startswith("etext"):
      return "%s/%s" % (mirror, name)
   if int(key) < 10:
      parts = "0/%s" % key
   else:
      parts = "%s/%s" % ("/".join(str(key)[:-1]), key)
   return "%s/%s/%s" % (mirror, parts, name)

# Extracts the value of the "Last-modified" header, formats is like the SQLite
# function datetime().
def get_last_modified(fp):
   last_mod = fp.info().get("last-modified")
   assert last_mod
   ret = parsedate(last_mod)
   assert ret
   # Allow a 1-day slop to account for time zones mismatches and errors in the
   # catalog (modification date not matching the real file modification date,
   # even after timezone adjustments, possibly because someone didn't use the
   # real file modification date). In practice, there is no reliable way to
   # determine if the file we intend to download is really the last available
   # version.
   ret = datetime.datetime(*ret[:6]) + datetime.timedelta(1)
   return time.strftime("%Y-%m-%d %H:%M:%S", ret.utctimetuple())

def download_ebook_text(url, enc, prev_mod, downloaded):
   with urlopen(url) as fp:
      last_mod = get_last_modified(fp)
      if not downloaded or last_mod >= prev_mod:
         data = fp.read()
         # Sloppy editing, as usual.
         encs = [enc, "utf-8", "iso-8859-1", "windows-1252", "iso-8859-15"]
         for enc in encs:
            try:
               return url, prev_mod, data.decode(enc)
            except UnicodeDecodeError:
               pass
         inform("cannot download '%s' (invalid encoding)" % url)
   return None, None, None

def download(key, name, encoding, prev_mod, downloaded):
   url = make_book_url(key, name)
   url, last_mod, text = download_ebook_text(url, encoding, prev_mod, downloaded)
   if not url:
      return
   text = remove_boilerplate(cleanup(text))
   text = zlib.compress(text.encode("UTF-8"), 9)
   return key, text, url, last_mod

MAX_RETRY = 3

def try_download(args):
   key, name, encoding, last_modified, downloaded = args
   tries = 0
   while True:
      try:
         return download(key, name, encoding, last_modified, downloaded)
      except KeyboardInterrupt:
         die()
      except (urllib.error.URLError, urllib.error.HTTPError, http.client.RemoteDisconnected):
         if tries >= MAX_RETRY:
            inform("cannot download '%s': connection error" % key)
            return
         tries += 1

LIGATURES_TBL = str.maketrans({
   "œ": "oe",
   "æ": "ae",
})

def normalize(s):
   s = unicodedata.normalize("NFKC", s)
   # SQLite doesn't recognize non-ASCII whitespace.
   s = " ".join(s.split())
   # SQLite doesn't support Unicode casefolding. In the context of a query, we
   # can't just casefold the whole string, because the case of query operators
   # is significant. Query operators being ASCII strings, and SQLite being able
   # to casefold ASCII strings, we only bother to casefold non-ASCII code
   # points.
   s = "".join(c in ascii_uppercase and c or c.casefold() for c in s)
   # Ligatures not covered by NFKC.
   s = s.translate(LIGATURES_TBL)
   return s

def make_document(fields):
   doc = {}
   for field, values in fields.items():
      if isinstance(values, (str, int)):
         values = [str(values)]
      doc[field] = " ".join(normalize(value) for value in values)
   return doc

def iter_catalog(url):
   try:
      fp = urlopen(url)
   except ValueError:
      # Maybe a file name.
      url = "file://%s" % os.path.abspath(url)
      fp = urlopen(url)
   except urllib.error.HTTPError as e:
      if e.getcode() == 403:
         die("downloads blocked, retry tomorrow")
      raise
   try:
      tf = tarfile.open(mode="r|bz2", fileobj=fp)
   except tarfile.ReadError:
      # Most likely issue.
      die("cannot read catalog; too much downloads?")
   while True:
      tinfo = tf.next()
      if not tinfo:
         break
      key = os.path.basename(os.path.dirname(tinfo.name))
      if key.isdigit():
         yield int(key), tf.extractfile(tinfo)


class Gutenberg(object):

   def __init__(self, path=DB_PATH, catalog_url=CATALOG_URL,
                num_workers=DOWNLOAD_POOL_SIZE):
      self.path = os.path.expandvars(os.path.expanduser(path))
      self.catalog_url = catalog_url
      self.num_workers = num_workers
      self.conn = sqlite3.connect(self.path)
      cur = self.conn.cursor()
      cur.executescript(SCHEMA)
      if not cur.execute("SELECT value FROM Infos WHERE key = 'last_catalog_update'").fetchone():
         self.update_catalog()

   def update_catalog(self):
      inform("updating catalog")
      cur = self.conn.cursor()
      cur.executescript("DELETE FROM Metadata; DELETE FROM Search;")
      for key, fp in iter_catalog(self.catalog_url):
         name, enc, last_mod, fields = parse_xml(fp, key)
         if name is None:
            continue
         fields["key"] = key
         cur.execute("""INSERT INTO Metadata(
            key, metadata, name, encoding, last_modified
         ) VALUES(?, ?, ?, ?, ?)""",
         (key, json.dumps(fields, ensure_ascii=False), name, enc, last_mod))
         doc = make_document(fields)
         cur.execute("""INSERT INTO SEARCH(
            key, language, author, title, subject
         ) VALUES(:key, :language, :author, :title, :subject)""", doc)
      cur.execute("""INSERT OR REPLACE INTO Infos(key, value)
         VALUES('last_catalog_update', datetime('now'))""")
      self.conn.commit()

   def search(self, query):
      query = normalize(str(query)) 
      for (metadata,) in self.conn.execute("""SELECT metadata
         FROM Metadata NATURAL JOIN Search WHERE Search match ?""", (query,)):
         yield json.loads(metadata)
   
   def text(self, query):
      query = normalize(str(query))      
      for (blob,) in self.conn.execute("""SELECT contents
         FROM Data NATURAL JOIN Search WHERE Search match ?""", (query,)):      
         yield zlib.decompress(blob).decode()

   def file(self, query):
      query = normalize(str(query))
      for (author, title, blob) in self.conn.execute("""SELECT author, title, contents
         FROM Data NATURAL JOIN Search WHERE Search match ?""", (query,)):
         yield author, title, blob

   def queries(self):
      for (q,) in self.conn.execute("""SELECT query FROM DownloadQueries
         ORDER BY last_issued DESC"""):
         yield q
   
   def forget(self, query):
      query = normalize(str(query))
      self.conn.execute("DELETE FROM DownloadQueries WHERE query = ?", (query,))
      self.conn.commit()

   def download(self, query):
      query = normalize(str(query))
      cur = self.conn.cursor()
      cur.execute("""
      INSERT OR REPLACE INTO DownloadQueries(query, last_issued)
         VALUES (?, datetime('now'))""", (query,))
      self.conn.commit()
      keys = list(cur.execute("""
      SELECT Metadata.key, Metadata.name,
             Metadata.encoding, Metadata.last_modified,
             Data.when_downloaded
      FROM Search INNER JOIN Metadata ON Search.key = Metadata.key
                  LEFT OUTER JOIN Data ON Metadata.key = Data.key
      WHERE Search MATCH ?
            AND Metadata.last_modified > COALESCE(Data.last_modified, '')
      """, (query,)))
      if keys:
         self.download_keys(keys)

   def update(self):
      cur = self.conn.cursor()
      # Update the catalog if need be.
      if cur.execute("""SELECT datetime('now', '-1 day') > datetime(value)
         FROM Infos WHERE key = 'last_catalog_update'""").fetchone()[0]:
         self.update_catalog()
      # We download new files first, then update the ones we've already
      # downloaded.
      keys = list(cur.execute("""
      SELECT Metadata.key, Metadata.name,
             Metadata.encoding, Metadata.last_modified,
             COALESCE(Data.when_downloaded, '') AS downloaded
      FROM Search INNER JOIN Metadata ON Search.key = Metadata.key
                  INNER JOIN DownloadQueries
                  LEFT OUTER JOIN Data ON Metadata.key = Data.key
      WHERE Search MATCH query
            AND Metadata.last_modified > COALESCE(Data.last_modified, '')
      UNION
      SELECT Metadata.key, Metadata.name,
             Metadata.encoding, Metadata.last_modified,
             Data.when_downloaded AS downloaded
      FROM Metadata INNER JOIN Data ON Metadata.key = Data.key
      WHERE Metadata.last_modified > Data.last_modified
      ORDER BY downloaded"""))
      if keys:
         self.download_keys(keys)
   
   def download_keys(self, keys):
      p = Pool(self.num_workers)
      itor = p.imap_unordered(try_download, keys)
      cur = self.conn.cursor()
      nr = 0
      try:
         for i, data in enumerate(itor, 1):
            if data:
               cur.execute("""INSERT OR REPLACE
               INTO Data(key, contents, url, last_modified, when_downloaded)
               VALUES(?, ?, ?, ?, datetime('now'))""", data)
               nr += 1
               if nr % 10 == 0:
                  self.conn.commit()
            progress(i, len(keys), self.num_workers)
         self.conn.commit()
      finally:
         progress_finish()

def cmd_search(argv):
   from collections import OrderedDict
   ordered_keys = ["key", "author", "title", "language", "subject"]
   for doc in Gutenberg().search(argv[0]):
      doc = OrderedDict((k, doc[k]) for k in ordered_keys)
      print(json.dumps(doc, ensure_ascii=False))

def cmd_text(argv):
   for text in Gutenberg().text(argv[0]):
      sys.stdout.write(text)

def cmd_file(argv):
   for author, title, blob in Gutenberg().file(argv[0]):
      normalized_author = slugify(author)[:32]
      normalized_title = slugify(title)[:48]
      if not os.path.exists(normalized_author):
         print(f"create directory: {normalized_author}")
         os.makedirs(normalized_author)
      target = os.path.join(normalized_author, normalized_title) + ".txt"
      if not os.path.exists(target):
         print(f"create file: {target}")
         with open(target, "w") as f:
            f.write(zlib.decompress(blob).decode())
      else:
         print(f"file exists, skipping: {target}")

def cmd_download(argv):
   Gutenberg().download(argv[0])

def cmd_update(argv):
   Gutenberg().update()

def cmd_forget(argv):
   Gutenberg().forget(argv[0])

def cmd_queries(argv):
   for q in Gutenberg().queries():
      print(q)

COMMANDS = {
   "search": {"func": cmd_search, "argc": 1},
   "text": {"func": cmd_text, "argc": 1},
   "file": {"func": cmd_file, "argc": 1},
   "download": {"func": cmd_download, "argc": 1},
   "queries": {"func": cmd_queries, "argc": 0},
   "update": {"func": cmd_update, "argc": 0},
   "forget": {"func": cmd_forget, "argc": 1},
}

USAGE = """\
Usage: %s <command> ...
Access ebooks from the Project Gutenberg.

Search commands:
   search <query>    display metadata of ebooks matching a query
   text <query>      display the contents of downloaded ebooks matching a query
   file <query>      save downloaded ebooks to disk, as [AUTHOR]/[TITlE].txt 
   queries           display a list of submitted download queries

Download commands:
   download <query>  download all ebooks matching a query
   forget <query>    don't download new ebooks matching a submitted query
   update            update the catalog, download new ebooks matching submitted
                       queries, update downloaded ebooks that have been emended"""

def usage():
   me = os.path.basename(sys.argv[0])
   print(USAGE % me, file=sys.stderr)
   die()

if __name__ == "__main__":
   argc, argv = len(sys.argv), sys.argv
   if argc < 2:
      usage()
   cmd = COMMANDS.get(argv[1])
   if not cmd or argc - 2 != cmd["argc"]:
      usage()
   try:
      cmd["func"](argv[2:])
   except (KeyboardInterrupt, BrokenPipeError):
      die()
