# Environment Setup

[Pixi]: https://pixi.sh

The Git repository includes everything you need to run these tools, except for
the data, the [Pixi][] package/environment manager, and `git` itself.  To get
started, clone the repository:

```console
$ git clone https://github.com/inertia-lab/bookdata-tools.git
```

## System Requirements

You will need:

- A Unix-like environment (macOS or Linux)
- [Pixi][]
- 250GB of disk space
- At least 24 GB of memory (lower may be possible)

## Import Tool Dependencies

The import tools are written in Python and Rust.  The provided [Pixi][] dependency
and lock files provide all you need to set up the environment.

You can install Pixi with their [installation
instructions](https://pixi.sh/latest/installation/) on Linux and macOS:

```console
$ curl -fsSL https://pixi.sh/install.sh | sh
```

Pixi is also available through Homebrew, so `brew install pixi` is the easiest
way to get it on macOS.

Once you have installed Pixi, you can install the environment:

```console
$ pixi install
```

You can then either use `pixi run` to run individual commands, or you can spawn
a new shell with the dependencies loaded with `pixi shell`.

All tool dependencies are specified in `pixi.toml`.
