usage() {
    echo "./create_unit_test_files.sh <build directory>"
}

if [[ $# != 1 ]]; then
    usage
    exit 1
fi

build_directory=$1

if [[ ! -f $build_directory ]]; then
    mkdir $build_directory
fi

wget -O tmp http://www.gutenberg.org/files/2600/2600-0.txt

# create zero file
touch $build_directory/zero_file

# create small file
cp tmp ${build_directory}/small_file
truncate --size 148 ${build_directory}/small_file

# create medium file
for i in {1..4}; do
    cat tmp >> ${build_directory}/medium_file
done

# create large file
cp tmp $build_directory
for i in {1..7}; do
    cat ${build_directory}/tmp >> ${build_directory}/tmp2
    cat ${build_directory}/tmp >> ${build_directory}/tmp2
    mv ${build_directory}/tmp2 ${build_directory}/tmp
done
mv ${build_directory}/tmp ${build_directory}/large_file

rm tmp
