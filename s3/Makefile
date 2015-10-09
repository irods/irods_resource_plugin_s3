TARGET = libs3.so

SRCS = libirods_s3.cpp

HEADERS = libirods_s3.hpp

EXTRALIBS = /usr/lib/libirods_client.a -ls3 -lxml2 -lcurl

EXTRAINCS = 

######################################################################
# General settings below.
# These should not need to be changed in most cases.

SODIR = ..
OBJDIR = .objs
DEPDIR = .deps

FULLTARGET = ${SODIR}/${TARGET}
DEPFILE = .depends

OBJS = $(patsubst %.cpp, ${OBJDIR}/%.o, ${SRCS})
DEPS = $(patsubst %.cpp, ${DEPDIR}/%.d, ${SRCS})

GCC = g++ -DRODS_SERVER

INC = -I/usr/include/irods/boost -I/usr/include/irods -I/usr/include/openssl ${EXTRAINCS}

.PHONY: clean

default: ${FULLTARGET}

clean:
	@-rm -f ${FULLTARGET} > /dev/null 2>&1
	@-rm -f ${OBJS} > /dev/null 2>&1
	@-rm -f ${DEPS} > /dev/null 2>&1
	@-rm -f ${DEPFILE} > /dev/null 2>&1

${FULLTARGET}: ${OBJS} ${DEPFILE}
	@-mkdir -p ${SODIR} > /dev/null 2>&1
	${GCC} ${INC} -fPIC -shared -o ${FULLTARGET} ${OBJS} ${EXTRALIBS}

${DEPFILE}: ${DEPS}
	@-rm -f ${DEPFILE} > /dev/null 2>&1
	@for dep in ${DEPS}; do \
	cat $$dep >> ${DEPFILE}; \
	done

${DEPS}: ${SRCS} ${HEADERS}
	@-mkdir -p ${DEPDIR} > /dev/null 2>&1
	${GCC} ${INC} -MM $< -MT $(patsubst %.cpp, ${OBJDIR}/%.o, $<) -MF $@

${OBJDIR}/%.o: %.cpp
	@-mkdir -p ${OBJDIR} > /dev/null 2>&1
	${GCC} ${INC} -O3 -fPIC -c -o $@ $<

include ${DEPFILE}

