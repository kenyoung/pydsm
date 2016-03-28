dsm.so: pydsm.c ./Makefile
	gcc -O3 -Wall -fPIC -shared -I/usr/local/anaconda/include/python2.7 /usr/local/anaconda/lib/libpython2.7.so \
	-o pydsm.so pydsm.c /common/lib/libdsm.a -lpthread -lrt
