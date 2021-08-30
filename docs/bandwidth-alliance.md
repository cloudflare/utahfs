Bandwidth Alliance
------------------

If you use Backblaze B2 as your storage provider, you can possibly reduce the
cost of your UtahFS deployment by taking advantage of the fact that Backblaze is
a Bandwidth Alliance member. This means they'll waive the cost of download
bandwidth for files downloaded through Cloudflare's edge.

To do this:

1. Make sure the bucket that your files are stored in is configured to be
   publicly accessible.
2. Click on "Browse Files", click on your bucket, and upload some file if
   there's not already one. Click on the file to bring up the info prompt. Take
   the "Friendly URL" and remove the filename from the end and the trailing
   slash. It should end with the bucket name. Save this as `b2-url` in your
   config but replace the hostname with your zone.
3. In your Cloudflare account, add an orange-clouded CNAME record pointing to
   the hostname in the "Friendly URL".

So for example if your "Friendly URL" is:
`https://f002.backblazeb2.com/file/bucket-name-here` and your zone is
`utahfs.example.com`, then in your Cloudflare account you'd add a CNAME for the
label `utahfs` pointing to `f002.backblazeb2.com` and save
`https://utahfs.example.com/file/bucket-name-here` in your config.
