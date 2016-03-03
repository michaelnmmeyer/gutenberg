# gutenberg

Download ebooks from the Project Gutenberg.

## Purpose

This is a helper script for downloading plain text ebooks from the Project
Gutenberg. It can do the following:

*  Query the Gutenberg catalog with a simple [full-text search
   syntax](https://www.sqlite.org/fts3.html#section_3).
*  Download ebooks matching a query, performing HTTP requests in parallel and
   dispatching them among Gutenberg mirrors.
*  Normalize ebooks metadata and contents, strip legal boilerplate.
*  Automatically download new ebooks matching submitted queries, keep the local
   database up-to-date.

## Installation

Python3 is required. On Debian and derivatives, you can install it with:

    sudo apt-get install python3

Then, if you want to download the full repository:

    git clone https://github.com/michaelnmmeyer/gutenberg
    sudo make -C gutenberg install

Alternatively, you can just copy the script to some location in your `PATH`:

    wget https://raw.githubusercontent.com/michaelnmmeyer/gutenberg/master/gutenberg.py
    sudo install -pm 0755 gutenberg.py /usr/local/bin/gutenberg
    rm gutenberg.py

## Basics

Say we're interested in Italian ebooks about history. We can display a list of
matching ebooks as follows:

    $ gutenberg search "language:it AND subject:history"

(The Gutenberg catalog is downloaded when the database is created, so the above
will take a few minutes at first. Subsequent queries will be faster. Afterwards,
the catalog will be updated every week.)

If results seem legit, we can download all matching ebooks with:

    $ gutenberg download "language:it AND subject:history"

The above command both downloads all ebooks that currently match the query and
save the query itself for future use.

If new ebooks on the subject we're interested in are created or existing one
are emended, we'd like our local database to reflect these changes. This can
be done by issuing the following from time to time:

    $ gutenberg update

The above executes again all submitted download queries, looking for newly added
ebooks, and updates already downloaded ebooks.

If, for some reason, we're not interested anymore in a subject, we can stop
automatically downloading new ebooks about it by issuing the following:

    $ gutenberg forget "language:it AND subject:history"

The above deletes the submitted query from the database, so that it won't be
executed again when `gutenberg update` is called. Downloaded ebooks will still
be updated.

To display all currently active queries, use the following:

    $ gutenberg queries

Finally, to display the contents of the ebooks we just downloaded, we can issue
the following:

    $ gutenberg text "language:it AND subject:history"

This displays on the standard output the contents of all ebooks matching the
submitted query, as a single concatenated file. To display the contents of a
particular ebook, the simplest solution is to use its identifier:

    $ gutenberg text key:10215

## Example queries

Download all Shakespeare's works:

    $ gutenberg download author:shakespeare

Download all ebooks that have some relation with Shakespeare:

    $ gutenberg download shakespeare

Download all French ebooks:

    $ gutenberg download language:fr

Download all French ebooks, except those from Proust:

    $ gutenberg download "language:fr NOT author:proust"
   
Download the original French text of Proust's *Swann*:

    $ gutenberg download "language:fr AND author:proust AND title:swann"

Download all ebooks in German or about Germany:

    $ gutenberg download "language:de OR subject:germany"

Download an ebook given its identifier:

    $ gutenberg download key:573 

## Database structure

Downloaded data is stored in a single SQLite database, which, per default, is
created at `~/.gutenberg`. Its schema is the following:

    /* Informations about the state of the database.
     * Possible keys are:
     * - last_catalog_update: last day the Gutenberg catalog was updated. If not
     *   present or if the catalog has not been updated in a while, the catalog is
     *   updated at startup.
     */
    CREATE TABLE IF NOT EXISTS Infos(
       key TEXT PRIMARY KEY UNIQUE NOT NULL,
       value TEXT NOT NULL
    );
    
    /* List of issued download queries.
     * - last_issued: last time the query was issued, not necessarily the last time
     *   all ebooks matching the query have been downloaded.
     */
    CREATE TABLE IF NOT EXISTS DownloadQueries(
       query TEXT PRIMARY KEY UNIQUE NOT NULL,
       last_issued DATETIME NOT NULL
    );
    
    /* Ebooks metadata.
     * This is constructed from the Gutenberg catalog. Ebooks that are not available
     * as plain text are discarded. Columns meaning:
     * - key: the ebook identifier.
     * - metadata: metadata extracted from the Gutenberg catalog. This is a JSON
     *   document. It contains the following fields:
     *   - author: list of authors
     *   - title: book title, as a list of strings. There is one string per title
     *     line. When a book title spans multiple lines, it is often the case that
     *     the title proper is on the first line, and subtitles follow.
     *   - language: list of languages
     *   - subject: list of subjects
     *   All strings are encoded to UTF-8 and normalized to NFC.
     * - url: where to find the book (plain-text UTF-8 version) on the Gutenberg
     *   website. This URL is not used for downloads because there are limitations
     *   on the number of downloadable ebooks per day.
     */
    CREATE TABLE IF NOT EXISTS Metadata(
       key INTEGER PRIMARY KEY UNIQUE NOT NULL,
       metadata TEXT NOT NULL,
       url TEXT UNIQUE NOT NULL
    );
    
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
     * - last_modified: date of last modification, as reported by the server from
     *   which the ebook was downloaded.
     * - when_downloaded: when the ebook was downloaded.
     */
    CREATE TABLE IF NOT EXISTS Data(
       key INTEGER PRIMARY KEY UNIQUE NOT NULL,
       contents BLOB NOT NULL,
       url TEXT UNIQUE NOT NULL,
       last_modified DATETIME NOT NULL,
       when_downloaded DATETIME NOT NULL
    );
