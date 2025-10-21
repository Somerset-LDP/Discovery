from common.cohort_membership import is_cohort_member

def run():
    pass

# read GP data from raw S3 bucket
# extract NHS number
# check cohort membership
# if in cohort, 
#   extract Ethnicity
#   replace with synthetic data
# write modified GP data to IG conformant S3 bucket
# delete GP data from raw S3 bucket