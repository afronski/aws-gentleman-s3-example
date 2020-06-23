# You can encounter problems with installing cryptograhy
# on Debian/Ubuntu package via pip, solution:
#
# - Debian/Ubuntu: sudo apt-get install build-essential libssl-dev libffi-dev python-dev
# - Fedora/Centos7: sudo yum install gcc libffi-devel python-devel openssl-devel

all: install

install:
	virtualenv .env --no-site-packages --python=python3.7
	.env/bin/python -m pip install -r requirements.txt

clean:
	rm -rf .env/
