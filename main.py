import sys

from filetransfer import cli, gui


def main() -> int:
    if len(sys.argv) > 1:
        return cli.main()
    gui.main()
    return 0


if __name__ == "__main__":
    sys.exit(main())
