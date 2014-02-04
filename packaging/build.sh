#!/bin/bash -e

# setup
STARTTIME="$(date +%s)"
SCRIPTNAME=`basename $0`
SCRIPTPATH=$( cd $(dirname $0) ; pwd -P )
FULLPATHSCRIPTNAME=$SCRIPTPATH/$SCRIPTNAME
TOPLEVEL=$( cd $SCRIPTPATH/../ ; pwd -P )
cd $SCRIPTPATH

USAGE="
Usage:
  $SCRIPTNAME
  $SCRIPTNAME clean
"

# Color Manipulation Aliases
if [[ "$TERM" == "dumb" || "$TERM" == "unknown" ]] ; then
    text_bold=""      # No Operation
    text_red=""       # No Operation
    text_green=""     # No Operation
    text_yellow=""    # No Operation
    text_blue=""      # No Operation
    text_purple=""    # No Operation
    text_cyan=""      # No Operation
    text_white=""     # No Operation
    text_reset=""     # No Operation
else
    text_bold=$(tput bold)      # Bold
    text_red=$(tput setaf 1)    # Red
    text_green=$(tput setaf 2)  # Green
    text_yellow=$(tput setaf 3) # Yellow
    text_blue=$(tput setaf 4)   # Blue
    text_purple=$(tput setaf 5) # Purple
    text_cyan=$(tput setaf 6)   # Cyan
    text_white=$(tput setaf 7)  # White
    text_reset=$(tput sgr0)     # Text Reset
fi

# =-=-=-=-=-=-=-
# boilerplate
echo "${text_cyan}${text_bold}"
echo "+------------------------------------+"
echo "| iRODS Plugin Build Script          |"
echo "+------------------------------------+"
date
echo "${text_reset}"

# =-=-=-=-=-=-=-
# translate long options to short
for arg
do
    delim=""
    case "$arg" in
        --coverage) args="${args}-c ";;
        --help) args="${args}-h ";;
        --release) args="${args}-r ";;
        # pass through anything else
        *) [[ "${arg:0:1}" == "-" ]] || delim="\""
        args="${args}${delim}${arg}${delim} ";;
    esac
done
# reset the translated args
eval set -- $args
# now we can process with getopts
while getopts ":chr" opt; do
    case $opt in
        c)
        COVERAGE="1"
        echo "-c detected -- Building plugin with coverage support (gcov)"
        ;;
        h)
        echo "$USAGE"
        ;;
        r)
        RELEASE="1"
        echo "-r detected -- Building for release"
        ;;
        \?)
        echo "Invalid option: -$OPTARG" >&2
        ;;
    esac
done
echo ""

# =-=-=-=-=-=-=-
# check arguments
if [ $# -gt 1 ] ; then
    echo "$USAGE" 1>&2
    exit 1
fi
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ] ; then
    echo "$USAGE"
    exit 0
fi

# =-=-=-=-=-=-=-
# detecting build environment
echo "${text_green}${text_bold}Build Environment...${text_reset}"
# =-=-=-=-=-=-=-
# determine the OS Flavor
DETECTEDOS=`$TOPLEVEL/packaging/find_os.sh`
echo "Detected OS                         [$DETECTEDOS]"
# =-=-=-=-=-=-=-
# determine the OS Version
DETECTEDOSVERSION=`$TOPLEVEL/packaging/find_os_version.sh`
echo "Detected OS Version                 [$DETECTEDOSVERSION]"
# =-=-=-=-=-=-=-
# detect the project name
source $TOPLEVEL/VERSION
echo "Detected Plugin Name                [$PLUGINNAME]"
EPM_PACKAGENAME=${PLUGINNAME//_/-}
echo "Detected EPM Package Name           [$EPM_PACKAGENAME]"
# =-=-=-=-=-=-=-
# detect plugin version
echo "Detected Plugin Version to Build    [$PLUGINVERSION]"
echo "Detected Plugin Version Integer     [$PLUGINVERSIONINT]"
# =-=-=-=-=-=-=-
# get into the top level directory
cd $TOPLEVEL
echo "Detected Project Directory          [$TOPLEVEL]"
# =-=-=-=-=-=-=-
# set packaging directory
PACKAGEDIR="$TOPLEVEL/packaging"
echo "Detected Packaging Directory        [$PACKAGEDIR]"
# =-=-=-=-=-=-=-
# set build directory
BUILDDIR="$TOPLEVEL/build"
echo "Detected Target Build Directory     [$BUILDDIR]"
# =-=-=-=-=-=-=-
# define list file
LISTFILE=$PACKAGEDIR/$PLUGINNAME.list
echo "Detected EPM List File              [$LISTFILE]"

# =-=-=-=-=-=-=-
# check for clean
if [ $# -eq 1 ] ; then
    if [ "$1" == "clean" ] ; then
        # clean up any build-created files
        echo "${text_green}${text_bold}Cleaning...${text_reset}"
        rm -f $LISTFILE
        rm -rf linux-2.*
        rm -rf linux-3.*
        rm -rf macosx-10.*
        rm -rf $BUILDDIR
        make clean
        echo "${text_green}${text_bold}Done.${text_reset}"
        exit 0
    fi
fi

# =-=-=-=-=-=-=-
# require irods-dev package
if [ ! -f /usr/lib/libirods.a ] ; then
    echo ""
    echo "ERROR :: \"irods-dev\" package required to build this plugin" 1>&2
    exit 1
fi

# =-=-=-=-=-=-=-
# detect number of cpus
if [ "$DETECTEDOS" == "MacOSX" ] ; then
    DETECTEDCPUCOUNT=`sysctl -n hw.ncpu`
elif [ "$DETECTEDOS" == "Solaris" ] ; then
    DETECTEDCPUCOUNT=`/usr/sbin/psrinfo -p`
else
    DETECTEDCPUCOUNT=`cat /proc/cpuinfo | grep processor | wc -l | tr -d ' '`
fi
if [ $DETECTEDCPUCOUNT -lt 2 ] ; then
    DETECTEDCPUCOUNT=1
fi
CPUCOUNT=$(( $DETECTEDCPUCOUNT + 3 ))
MAKEJCMD="make -j $CPUCOUNT"
echo "Detected CPUs                       [$DETECTEDCPUCOUNT]"
echo "Compile Command                     [$MAKEJCMD]"
echo ""

# =-=-=-=-=-=-=-
# build the plugin itself
echo "${text_green}${text_bold}Building...${text_reset}"
$MAKEJCMD

# =-=-=-=-=-=-=-
# generate EPM list file from the template
echo ""
echo "${text_green}${text_bold}Creating Package...${text_reset}"
cd $TOPLEVEL
sed -e "s,TEMPLATE_PLUGINVERSIONINT,$PLUGINVERSIONINT,g" $LISTFILE.template > $LISTFILE.tmp
mv $LISTFILE.tmp $LISTFILE
sed -e "s,TEMPLATE_PLUGINVERSION,$PLUGINVERSION,g" $LISTFILE > $LISTFILE.tmp
mv $LISTFILE.tmp $LISTFILE

# =-=-=-=-=-=-=-
# detect architecture
unamem=`uname -m`
if [[ "$unamem" == "x86_64" || "$unamem" == "amd64" ]] ; then
    arch="amd64"
else
    arch="i386"
fi

# =-=-=-=-=-=-=-
# set coverage flags
if [ "$COVERAGE" == "1" ] ; then
    # sets EPM to not strip binaries of debugging information
    EPMOPTS="-g"
    # sets listfile coverage options
    EPMOPTS="$EPMOPTS COVERAGE=true"
else
    EPMOPTS=""
fi

# =-=-=-=-=-=-=-
# build package
cd $TOPLEVEL
EPMCMD=/usr/bin/epm
if [ "$DETECTEDOS" == "RedHatCompatible" ] ; then # CentOS and RHEL and Fedora
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS RPMs${text_reset}"
    EXTENSION="rpm"
    epmvar="REDHAT"
    ostype=`awk '{print $1}' /etc/redhat-release`
    osversion=`awk '{print $3}' /etc/redhat-release`
    if [ "$ostype" == "CentOS" -a "$osversion" \> "6" ]; then
        epmosversion="CENTOS6"
        SUFFIX="centos6"
    else
        epmosversion="NOTCENTOS6"
        SUFFIX="redhat"
    fi
    $EPMCMD $EPMOPTS -f rpm $EPM_PACKAGENAME RPM=true $epmosversion=true $LISTFILE

elif [ "$DETECTEDOS" == "SuSE" ] ; then # SuSE
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS RPMs${text_reset}"
    EXTENSION="rpm"
    SUFFIX="suse"
    epmvar="SUSE"
    $EPMCMD $EPMOPTS -f rpm $EPM_PACKAGENAME $epmvar=true $LISTFILE

elif [ "$DETECTEDOS" == "Ubuntu" -o "$DETECTEDOS" == "Debian" ] ; then  # Ubuntu
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS DEBs${text_reset}"
    EXTENSION="deb"
    epmvar="DEB"
    $EPMCMD $EPMOPTS -a $arch -f deb $EPM_PACKAGENAME $epmvar=true $LISTFILE

elif [ "$DETECTEDOS" == "Solaris" ] ; then  # Solaris
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS PKGs${text_reset}"
    EXTENSION="pkg"
    epmvar="PKG"
    $EPMCMD $EPMOPTS -f pkg $EPM_PACKAGENAME $epmvar=true $LISTFILE

elif [ "$DETECTEDOS" == "MacOSX" ] ; then  # MacOSX
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS DMGs${text_reset}"
    EXTENSION="dmg"
    epmvar="OSX"
    $EPMCMD $EPMOPTS -f osx $EPM_PACKAGENAME $epmvar=true $LISTFILE

elif [ "$DETECTEDOS" == "ArchLinux" ] ; then  # ArchLinux
    echo "${text_green}${text_bold}Running EPM :: Generating $DETECTEDOS TGZs${text_reset}"
    EXTENSION="tar.gz"
    epmvar="ARCH"
    $EPMCMD $EPMOPTS -f portable $EPM_PACKAGENAME $epmvar=true $LISTFILE

else
    echo "${text_red}#######################################################" 1>&2
    echo "ERROR :: Unknown OS, cannot generate packages with EPM" 1>&2
    echo "#######################################################${text_reset}" 1>&2
    exit 1
fi

# =-=-=-=-=-=-=-
# move package to build directory
cd $TOPLEVEL
mkdir -p $BUILDDIR
mv linux*/$EPM_PACKAGENAME*.$EXTENSION $BUILDDIR/$EPM_PACKAGENAME-$PLUGINVERSION.$EXTENSION
if [ "$SUFFIX" != "" ] ; then
    mv $BUILDDIR/$EPM_PACKAGENAME-$PLUGINVERSION.$EXTENSION $BUILDDIR/$EPM_PACKAGENAME-$PLUGINVERSION-$SUFFIX.$EXTENSION
fi
echo ""
echo "$BUILDDIR:"
ls -l $BUILDDIR

# =-=-=-=-=-=-=-
# show timing
TOTALTIME="$(($(date +%s)-STARTTIME))"
echo "${text_cyan}${text_bold}"
echo "+------------------------------------+"
echo "| iRODS Plugin Build Script          |"
echo "|                                    |"
printf "|   Completed in %02dm%02ds              |\n" "$((TOTALTIME/60))" "$((TOTALTIME%60))"
echo "+------------------------------------+"
echo "${text_reset}"

# =-=-=-=-=-=-=-
# exit cleanly
exit 0
