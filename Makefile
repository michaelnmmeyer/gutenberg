PREFIX = /usr/local

all:

install:
	install -pm 0755 gutenberg.py $(PREFIX)/bin/gutenberg

uninstall:
	rm -f $(PREFIX)/bin/gutenberg
