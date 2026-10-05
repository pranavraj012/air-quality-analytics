# Pig parameter file.
#
# Used for quick iteration against a small sample:
#   pig -x mapreduce -m pig/params_test.pig -f pig/01_clean_and_pivot.pig
#
# Production run uses the defaults in the script itself:
#   pig -x mapreduce -f pig/01_clean_and_pivot.pig

INPUT = '/airquality/test_raw/*'
OUTPUT = '/airquality/test_cleaned'