Setup Scaleway
------------

1. Sign up for a [Scaleway](https://www.scaleway.com/) account.
2. Go to the [Object Storage](https://console.scaleway.com/object-storage/buckets) tab,
   click "Create bucket", give the bucket whatever name you want, choose the region
   closest to you, and click "Create a bucket".
3. Set the bucket name as `storage-provider.s3-bucket` in your config,
   and the region name as `storage-provider.s3-region`.
   (example: `fr-par` or `nl-ams`)
4. Go to [Credentials](https://console.scaleway.com/account/organization/credentials)
   and generate a new token pair. Save the access key as `storage-provider.s3-app-id`
   in your config, and the secret key as `storage-provider.s3-app-key`.
5. Choose the URL of your region from [the docs](https://www.scaleway.com/en/docs/object-storage-feature/),
   for example `https://s3.fr-par.scw.cloud`.
   Save it as `storage-provider.s3-url` in your config.
