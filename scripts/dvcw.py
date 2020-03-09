from dvc.main import main


if __name__ == '__main__':
    try:
        from bookdata import dvcpatch
        dvcpatch.patch()
    except ImportError:
        pass
    main()
