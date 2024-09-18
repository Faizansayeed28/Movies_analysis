import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3_data_input
S3_data_input_node1720368085210 = glueContext.create_dynamic_frame.from_catalog(database="movie_analysis_db", table_name="movies_input", transformation_ctx="S3_data_input_node1720368085210")

# Script generated for node data_quality_check
data_quality_check_node1720368228629_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
         IsComplete "imdb_rating",
       ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

data_quality_check_node1720368228629 = EvaluateDataQuality().process_rows(frame=S3_data_input_node1720368085210, ruleset=data_quality_check_node1720368228629_ruleset, publishing_options={"dataQualityEvaluationContext": "data_quality_check_node1720368228629", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1720368319776 = SelectFromCollection.apply(dfc=data_quality_check_node1720368228629, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1720368319776")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1720368323475 = SelectFromCollection.apply(dfc=data_quality_check_node1720368228629, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1720368323475")

# Script generated for node Conditional Router
ConditionalRouter_node1720368466976 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1720368323475,
  group_filters = [GroupFilter(name = "bad_record", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1720368467209 = SelectFromCollection.apply(dfc=ConditionalRouter_node1720368466976, key="default_group", transformation_ctx="default_group_node1720368467209")

# Script generated for node bad_record
bad_record_node1720368467231 = SelectFromCollection.apply(dfc=ConditionalRouter_node1720368466976, key="bad_record", transformation_ctx="bad_record_node1720368467231")

# Script generated for node filtering_records
filtering_records_node1720368713022 = ApplyMapping.apply(frame=default_group_node1720368467209, mappings=[("poster_link", "string", "poster_link", "string"), ("series_title", "string", "series_title", "string"), ("released_year", "string", "released_year", "string"), ("certificate", "string", "certificate", "string"), ("runtime", "string", "runtime", "string"), ("genre", "string", "genre", "string"), ("imdb_rating", "double", "imdb_rating", "double"), ("overview", "string", "overview", "string"), ("meta_score", "long", "meta_score", "long"), ("director", "string", "director", "string"), ("star1", "string", "star1", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("star4", "string", "star4", "string"), ("no_of_votes", "long", "no_of_votes", "long"), ("gross", "string", "gross", "string"), ("DataQualityRulesPass", "array", "DataQualityRulesPass", "array"), ("DataQualityRulesFail", "array", "DataQualityRulesFail", "array"), ("DataQualityRulesSkip", "array", "DataQualityRulesSkip", "array"), ("DataQualityEvaluationResult", "string", "DataQualityEvaluationResult", "string")], transformation_ctx="filtering_records_node1720368713022")

# Script generated for node ruleOtcome
ruleOtcome_node1720368342846 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1720368319776, connection_type="s3", format="json", connection_options={"path": "s3://movie-analysis-proj/ruleOutcome/", "partitionKeys": []}, transformation_ctx="ruleOtcome_node1720368342846")

# Script generated for node bad_record_target
bad_record_target_node1720368611241 = glueContext.write_dynamic_frame.from_options(frame=bad_record_node1720368467231, connection_type="s3", format="json", connection_options={"path": "s3://movie-analysis-proj/bad_record/", "partitionKeys": []}, transformation_ctx="bad_record_target_node1720368611241")

# Script generated for node redshift_load
redshift_load_node1720442199711 = glueContext.write_dynamic_frame.from_catalog(frame=filtering_records_node1720368713022, database="movie_analysis_db", table_name="destination_dev_movies_imdb_movies_rating", redshift_tmp_dir="s3://glue-sales-data-bucket-prac",additional_options={"aws_iam_role": "arn:aws:iam::891377121323:role/redshift_role"}, transformation_ctx="redshift_load_node1720442199711")

job.commit()
