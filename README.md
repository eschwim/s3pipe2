s3pipe2
======

A s3 upload/download utility which allows piped input & onput, as well as
parallelized uploads & downloads.

Named s3pipe2 instead of s3pipe, since Tobias Galitzien [ had already taken
that name ](https://github.com/tgal/s3pipe), and I couldn't think of a better
one (s3tube?  s3conduit? s3hose?!  ARGH!).

s3pipe2 differs from s3pipe in that it performs segmented uploads, instead of
a single monolithic one.  Thus, a piped transfer to S3 uses a staging directory
on the local machine to create individual segments which are then uploaded to
S3 in parallel with other segments being written.  This will result in an S3
directory consisting of multiple segments, as opposed to a single file.

This method has several advantages:

* Reading/writing from STDIN/STDOUT can happen in parallel with s3 transfers.
  When using other "pipe to s3" mechanisms, the scripts in question do not
  stream data to/from S3, due to buffering issues, but instead must 
* Transfers typically happen much faster.  A single s3 transfer, even using
  multipart-upload (sadly, there is no multipart-download yet) rarely
  fully utilizes your network connection.
* Transfers are more durable.  Every uploaded segment's MD5 checksum is
  compared with the segment on disk, which isn't possible when piping data
  directly to or from S3.  This allows quick recovery from S3 transfer mishaps
  (which do happen), instead of discovering after you've uploaded 10TB of data
  that you have to start all over again.
* You can effectively pipe date *through* s3.  By first starting an upload to
  s3, and then starting a download from s3 using the same path (but presumably
  on a different machine), you don't have to wait for the upload to finish
  prior to beginning the download.  This can be used as a "poor man's
  multicast" to mirror the data on one machine to a large number of other
  machines in a close-to-realtime manner, with the added benefit of having
  a permanent archive of the transfer left in s3 after the transaction is
  completed.

The largest disadvantage to using s3pipe2 is the fact that the transfer is
stored as a segmented manifest in s3, as opposed to a single file.  This means
that re-assembling an upload made with s3pipe2 is difficult to do without
using s3pipe2 (although it *is* possible.  You just need to sync all of the
segments from the s3 folder to a location on disk, and then cat the files
together, i.e. `cat 0* > reassembled_file_name`).

Another disadvantage is that s3pipe2 will require local disk space in order
to stage files while they are being written & uploaded.  In general, the 
maximum amount of disk space required will equal the segment size (set using
the `-m` flag; 100MB, by default) multiplied by the number of s3 workers (set 
via the `-n` flag, with a default of 2).  I.e. using the default settings, the
staging directory (created under `/tmp` by default, although this can be 
changed via the `-p` flag) will need to have at least 200MB of free space.

To futher optimize the transfer process, consider using a tmpfs or other
memory-backed storage device to host the staging directory, as this will 
increase the rate at which segments are staged to disk, and reduce the 
likelihood of I/O contention with other processes on the system running 
s3pipe2.


## Installation

This is just a single-file script, so just just download and run it.  It will
need a semi-recent version of the "boto" python module to be installed,
though, so either:

    pip install boto

_or_

    yum install python-boto

_or_

    apt-get install python-boto

Then:

    wget https://raw.github.com/eschwim/s3pipe2/master/s3pipe2
    chmod 755 s3pipe2

## Supplying s3pipe2 with AWS credentials

While you can specify AWS credentials on the command when running s3pipe2,
it is definitely not recommended, as this exposes your credentials to people
with access to your process table.  Instead, it is preferable to save your 
credentials in the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment
variables.  Alternatively, s3pipe2 will attempt to read your credentials from
your ~/.aws/credentials and ~/.aws/config files, in that order.

## Usage

```
usage: s3pipe2 [options] <to|from> <path>

s3pipe2 v0.1: Pipe data to/from AWS S3

positional arguments:
  {to,from}        Pipe data to/from S3
  path             S3 path to upload/download from

optional arguments:
  -h, --help       show this help message and exit
  -a ACCESS-KEY    AWS access key to read/write with
  -s SECRET-KEY    AWS secret key to read/write with
  -r REGION        S3 region to use
  -m SEGMENT-SIZE  Size in MB of segments to upload (default 100MB)
  -b BUFFER-SIZE   Size in KB of read/write buffer to use (default 4KB)
  -p STAGING-PATH  Path to stage segments in
  -n NUM-WORKERS   # of parallel s3 upload/download workers to run (default 2)
  -t TRIES         # of times to attempt upload/download before quitting
  -v               Be verbose; print output to stderr
```

## Changelog

_v0.1_

    Initial version.
