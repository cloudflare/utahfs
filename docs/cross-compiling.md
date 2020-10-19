Cross-Compiling
---------------

Compiling for Linux on Mac:

```
$ brew install FiloSottile/musl-cross/musl-cross
$ CC=x86_64-linux-musl-gcc CXX=x86_64-linux-musl-g++ GOOS=linux CGO_ENABLED=1 go get github.com/cloudflare/utahfs/cmd/utahfs-server
```

Compiling for 64-bit ARM, with [xgo](https://github.com/karalabe/xgo):

```
xgo --targets=linux/arm64 github.com/cloudflare/utahfs/cmd/utahfs-server
```
