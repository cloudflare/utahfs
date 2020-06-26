Setup MinIO
-----------

MinIO is a self-hosted object storage database. It's the user's responsibility
to ensure that the server has enough capacity for their needs, and has
sufficient hardware redundancy to prevent data loss.

1. Setup a [MinIO](https://min.io/) server.
2. Set the MinIO access key and secret key as `s3-app-id` and `s3-app-key` in
   your config, respectively.
3. Login to the MinIO server and create a new bucket with whatever name you
   want. Set the bucket name as `s3-bucket` in your config.
4. Set the URL of the MinIO server as `s3-url` in your config, and set
   `s3-region` as `minio`.

Note that all the configuration options set here are kept under a
`storage-provider` key.
