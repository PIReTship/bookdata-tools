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
If you don't want to pay for cloud storage for hte data, there are several good
options for local hosting if you have a server with sufficient storage space:

-   [Garage][] and [Minio][] provide S3-compatible storage APIs.  Both store the
    data in an internal format (allowing checksums and deduplication), not in
    raw files on your file system, so you can only access the data through the
    S3 api.
-   [Caddy][] with the [webdav plugin][caddy-webdav] is the easiest way I have
    found to run a webdav server.  I've started moving towards webdav instead
    of S3 for in-house remotes so that the data can be accessed directly on the
    server filesystem.  Apache HTTPD also has good webdav support, but it is
    somewhat more cumbersome to configure.
:::

::: {.callout-note}
If you are a member of our research group, or a direct collaborator, using these
tools, contact Michael for access to our remote.
:::

[garage]: https://garagehq.deuxfleurs.fr/
[minio]: https://min.io/
[caddy]: https://caddyserver.com/
[caddy-webdav]: https://caddyserver.com/
