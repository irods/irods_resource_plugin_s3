# example:
# irule -F rules/move2DA.r "'s3resc;s3archive'" "'/tempZone/home/rods/testfile'" "'/my_bucket/home/rods/testfile'"
move_to_deep_archive {
	msicompound_archive_object(*resc_hier, *logical_path, *physical_path);
}
INPUT *resc_hier=$1, *logical_path=$2, *physical_path=$3
OUTPUT ruleExecOut
