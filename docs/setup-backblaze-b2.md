Setup Backblaze B2
------------------

1. Sign up for a [Backblaze](https://www.backblaze.com/) account.
2. Generate 8 bytes of randomness: `openssl rand -hex 8`
3. Create a bucket. The bucket should be public and be named the random
   string you generated above; also set the random string as `b2-bucket` in
   your config.
4. Click on "Lifecycle Settings", choose "Keep prior versions for 7 days",
   submit.
5. Click "App Keys" in the sidebar. Save the keyID for "Master Application Key"
   as `b2-acct-id` in your config. Select "Generate New Master Application Key"
   (or use the one you already know) and save the application key as
   `b2-app-key` in your config.
6. Click on "Browse Files", click on your bucket, upload some file. Click on
   the file to bring up the info prompt. Take the "Friendly URL" and remove
   the filename from the end and the trailing slash. It should end with the
   bucket name. Save this as `b2-url` in your config.
