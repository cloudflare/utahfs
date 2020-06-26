Setup Local Disk
----------------

Local disk storage keeps all data at a configured place on the user's local
drive. We only recommend you use this backend for testing! If you choose to use
it as a genuine backend, please ensure that you have sufficient hardware
redundancy (with a RAID) to prevent data loss.

1. Choose where you want data to be stored and set that path as `disk-path`
   under the `storage-provider` key in your config file.
