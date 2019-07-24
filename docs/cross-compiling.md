Cross-Compiling
---------------

Compiling for Linux on Mac:

```
$ brew install FiloSottile/musl-cross/musl-cross
$ CC=x86_64-linux-musl-gcc CXX=x86_64-linux-musl-g++ GOOS=linux CGO_ENABLED=1 go install code.cfops.it/~brendan/utahfs/cmd/utahfs-server
```
