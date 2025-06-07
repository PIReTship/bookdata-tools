# Data Storage

Once you have set up the [software environment](./setup.md), the one remaining
piece is to set up your data storage if you want to share the book data with
collaborators or between machines.  Since this project uses DVC, you will need
to configure a [DVC remote](https://dvc.org/doc/command-reference/remote/add) to
store your data. This will require around 200GB of space for all of the relevant
data files, in addition to the files in your local repository.

::: {.callout-note}
It is possible to work without a remote if you only need one copy of the data,
but as soon as you want to move the data between multiple machines or use DVC's
import facilities to load it into an experiment project, you will need a remote.
:::

Due to data redistribution restrictions we can't share access to the remote we
use within our research group.

What you need to do:

-   Add your remote (with `dvc remote add` or by editing `.dvc/config`). You can
    use any remote type supported by DVC.
-   Configure your remote as the default (with `dvc remote default`).

::: {.callout-tip}
If you don't want to pay for cloud storage for the data, there are several good
options for local hosting if you have a server with sufficient storage space:

-   DVC supports SSH remotes. These are much slower than S3 or WebDAV, but they
    work.
-   [rclone][] with [`rclone serve`][rclone-serve] is probably the easiest
    way to set up either a WebDAV or S3 remote.
-   For publicly-accessible repositories, [Caddy][] with the [webdav
    plugin][caddy-webdav] is a very good option to run a WebDAV server.  Apache
    HTTPD also has good WebDAV support, and it is what the INERTIA Lab uses, but
    it is somewhat more cumbersome to configure.
-   [Garage][] and [Minio][] provide S3-compatible storage APIs.  Both store the
    data in an internal format (allowing checksums and deduplication), not in
    raw files on your file system, so you can only access the data through the
    S3 API.
:::

::: {.callout-note}
If you are a member of our research group or a direct collaborator using these
tools, contact Michael for access to our remote.
:::

[garage]: https://garagehq.deuxfleurs.fr/
[minio]: https://min.io/
[caddy]: https://caddyserver.com/
[caddy-webdav]: https://github.com/mholt/caddy-webdav
[rclone]: https://rclone.org
[rclone-serve]: https://rclone.org/commands/rclone_serve/
