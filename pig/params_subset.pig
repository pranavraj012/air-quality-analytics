# Pig parameter file for the subset validation.
#
# Pig 0.17 has no -D flag for ad-hoc parameters; it reads them from a file
# passed with -m (see pig/params_test.pig for the other example).
#
# Used by run_subset.sh:
#   pig -x mapreduce -m pig/params_subset.pig -f pig/01_clean_and_pivot.pig

INPUT = '/airquality/subset/raw/data.csv'
OUTPUT = '/airquality/subset/cleaned/all'