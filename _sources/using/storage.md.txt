---
title: Data Storage
---

# Configuring Data Storage

Once you have set up the [software environment](./setup.md), the one remaining
piece is to set up your data storage.  Since this project uses DVC, you will
need to configure a [DVC
remote](https://dvc.org/doc/command-reference/remote/add) to store your data.
This will require around 200GB of space for all of the relevant data files, and
is in addition to the files in your local repository.

:::{note}
It is possible to get away without a remote if you only need one copy of the data,
but as soon as you want to move the data between multiple machines or use DVC's
import facilities to load it into an experiment project, you will need a remote.
:::

The repository is configured with our internal remote, but due to data redistribution
restrictions we can't share access to this remote.

What you need to do:

-   Remove our remote (with [`dvc remote remove`](https://dvc.org/doc/command-reference/remote/remove))
    or by editing `.dvc/config`).
-   Add your remote (with `dvc remote add` or by editing `.dvc/config`). You can use any remote type
    supported by DVC.
-   Configure your remote as the default (with `dvc remote default`).

:::{tip}
Since we have a server with a lot of disk space, our research group uses [MinIO](https://min.io/)
as our DVC remote.  It is an S3-compatible storage server that lets us use DVC with good performance
and without incurring the costs of storing in the Amazon public cloud.
:::

:::{note}
If you are a PIReT member using the tools, you can continue to use the configured remote, and
use `dvc pull` to fetch the data files without re-running everything.
:::
