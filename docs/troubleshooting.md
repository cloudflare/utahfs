Troubleshooting
---------------

If starting the client fails with this error, it may be because the target
directory is still 'mounted' by a client that was killed:

```
newConnection: Init: Reading init op: EOF
```

The resolution is to unmount the directory and try again:

```
$ sudo umount ./utahfs
```
