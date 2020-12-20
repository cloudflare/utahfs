UtahFS
======

UtahFS is a state-of-the art encrypted storage solution, meant to be similar to
Dropbox. It has a FUSE binding that creates a synthetic drive on the user's
computer that they can interact with like an external hard-drive. Files stored
in the drive are uploaded to a cloud storage provider, which means the drive
will never run out of space and minimizes the likelihood of any files being
lost. However the files are encrypted such that the cloud storage provider knows
almost nothing about what's being stored.


Features
--------

1. **Interchangeable Storage Providers.** For storing data in the cloud, UtahFS
   uses *Object Storage*, which is cheap and commodified. Example providers
   include: AWS S3, Google Cloud Storage, Backblaze B2, and Wasabi.
2. **Very Very Strong Encryption.** The method of encryption hides the number of
   files, file names, file contents, individual file size, and prevents any
   modifications (including rollbacks!). The only information which is clearly
   leaked is the maximum archive size: archives grow to fit new data, but won't
   shrink if that data is deleted. Instead, that space is left allocated and
   will be re-used if needed in the future.
3. **Local Hardware allows Multiple Users and Improved Performance.** If the
   user has server-like hardware on their LAN (Raspberry Pi / Intel NUC), this
   can be used to coordinate multiple users operating in the same archive. It
   can also dramatically improve the performance of uploads, because a user can
   upload large amounts of already-encrypted data to the server over the fast
   local network, and let the server take over the much slower upload to the
   cloud provider.
4. **Archive Mode.** The client can be configured to guard against deleting or
   overwriting existing files, while still allowing new files to be created and
   old files to be moved around. This helps protect against accidental data
   loss.
5. **Oblivious RAM (ORAM).**  ORAM can be used to hide the access pattern of
   data from the cloud storage provider, so the provider only sees the *amount*
   data is accessed. *(Access pattern: Which pieces of data are being accessed,
   and whether the access was a read or write.)*


How to Use
----------

Setup documentation can be found in the `docs/` folder.


Future Work
-----------

1. **Reliability strategies for the WAL.** Changes are buffered in a local
   Write-Ahead Log (WAL) before being sent to the cloud storage provider. If the
   disk that the WAL is stored on fails, it could become very difficult to
   return the archive to a usable, partially-regressed state.
