Setup Wasabi
------------

1. Sign up for a [Wasabi](https://wasabi.com/) account.
2. Click "Create Bucket", give the bucket whatever name you want, choose the
   region closest to you, and click "Create Bucket". Set the bucket name as
   `s3-bucket` in your config, and the region name as `s3-region`.
3. Click "Access Keys" on the sidebar, and then "Create New Access Key". Save
   the access key as `s3-app-id` in your config, and the secret key as
   `s3-app-key`.
4. Choose the URL of your region from
   [this](https://wasabi-support.zendesk.com/hc/en-us/articles/360015106031-What-are-the-service-URLs-for-Wasabi-s-different-regions-)
   support page. Prefix it with "https://" and save it as `s3-url` in your
   config.

Note that all the configuration options set here are kept under a
`storage-provider` key.
