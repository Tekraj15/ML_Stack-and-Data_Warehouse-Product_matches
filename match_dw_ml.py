import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType
import datetime
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
from awsglue.job import Job
from pyspark.sql.functions import split
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr, And, Or
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.functions import rank
import boto3
import pytz
import uuid
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import lit
import requests
import json
import os


#setting job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','domain','run_type'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
sc.setLogLevel("WARN")
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
domain  = str(args['domain'])
run_type  = str(args['run_type'])
today = datetime.now()
numOfPartitions = 80

print("Run Type",run_type)


def send_message_to_slack(message):
    print('here')
    url = "https://hooks.slack.com/services/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__1"
    payload={"channel": "#similar-product-update", "username": "Match Data Warehouse Glue Job", "text": message}
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response


def get_latest_s3_file_location(bucket,prefix,type_bucket):
    
    """
    Arguments : bucket : Name of Bucket  -> string
                prefix : Folder location -> string
    
    Output : recent file location -> string
    """
  
    client = boto3.client('s3')
    prefix = prefix + "/"
    
    while True:
        result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
        if result.get('CommonPrefixes'):
            if 'run_no' in result.get('CommonPrefixes')[-1]['Prefix'] and type_bucket == 'inf_pairs':
                return result.get('CommonPrefixes')[-1]['Prefix']
                
            elif 'day' in result.get('CommonPrefixes')[-1]['Prefix'] and type_bucket is None:
                return result.get('CommonPrefixes')[-1]['Prefix']
                
            elif 'load_date' in result.get('CommonPrefixes')[-1]['Prefix'] and type_bucket == 'match_lib':
                return result.get('CommonPrefixes')[-1]['Prefix']
            else:
                prefix = result.get('CommonPrefixes')[-1]['Prefix']
        else:
            return None
    

def get_string_index(split_list,string):
    """
    Desc : Get the index of a string in a list
    Arguments : split_list : List of split strings -> list
                string : string whose index needs to be found -> string
    
    Output : index -> int
    """
    
    index = [idx for idx, s in enumerate(split_list) if string in s][0]
    return index
    
    
def get_latest_file_date(bucket,prefix,type_bucket=None):
    """
    Desc : Given the location get the s3 file date
    Arguments : bucket : Name of Bucket -> string
                prefix : Folder location -> string
    
    Output : year,month,day -> tuple(string)
    """
   
    latest_file_location = get_latest_s3_file_location(bucket,prefix,type_bucket)
    if latest_file_location and type_bucket != 'match_lib':
        file_loc_split = latest_file_location.split("/")
        print("File Location Info :",file_loc_split)
    #if 'year' in latest_file_location and 'month' in latest_file_location and 'day' in latest_file_location:
        year = file_loc_split[get_string_index(file_loc_split,"year")].split("=")[1]
        month = file_loc_split[get_string_index(file_loc_split,"month")].split("=")[1]
        day = file_loc_split[get_string_index(file_loc_split,"day")].split("=")[1]
        if type_bucket == 'inf_pairs':
            run = file_loc_split[get_string_index(file_loc_split,"run_no")].split("=")[1]
            return (year,month,day,run)
        else:
            return (year,month,day)
        
    elif latest_file_location and type_bucket == 'match_lib':
        file_loc_split = latest_file_location.split("/")
        print("File Location Info :",file_loc_split)
    #if 'year' in latest_file_location and 'month' in latest_file_location and 'day' in latest_file_location:
        load_date = file_loc_split[get_string_index(file_loc_split,"load_date")].split("=")[1]
        return load_date
        
    else:
        return None
    
    
def gen_store(uuid):
    """
    Desc: given uuid generate store
    Arguments : uuid -> string
    Output : store -> string
    """
    
    store = uuid.split("_")[-1]
    return store + "_" + store
    
    
def gen_partitions(today):
    """
    Desc: Generate partitions(year,month,day) for writing records to s3
    output : (year,month,day) -> tuple(string)
    """
    
    curr_date = today
    year, month, day = str(curr_date.year), str(curr_date.month), str(curr_date.day)
    if len(month) < 2:
        month = "0" + month
    if len(day) < 2:
        day = "0" + day

    return (year,month,day)
    
    
def get_config():
    """
    Desc : Get Job Config from dynamodb
    Output : Rows of dynamodb -> List
    """

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("inference_pair_pruning_workflow")
    
    response = table.query(
    IndexName='domain-index',
    KeyConditionExpression=Key('domain').eq(domain))
    
    data = response['Items']

    return data
  
    
def gen_run_number(outputbucket,outputprefix,today):
    
        
    """
    Desc : Delete files if exist for the same day in the output s3 location
    Arguments : outputbucket : Name of Bucket -> string
                outputprefix : Folder location -> string
    
    """

    latest_date = get_latest_file_date(outputbucket,outputprefix,'inf_pairs')
    print(latest_date)
    if latest_date:
        year,month,day,run = latest_date
        
        if today.year == int(year) and today.month == int(month) and today.day == int(day):
            print('run',run)
            run = str(int(run) + 1)
            if len(run) < 2:
                run = "0" + run
            print("Adding New Run For Date {}/{}/{}, run {}".format(today.month,today.day,today.year,run))
            return run

        else:
            run = "01"
            print("New Run For Date {}/{}/{}, run {}".format(today.month,today.day,today.year,run))
            return run
        
    else:
        run = "01"
        print("New Run For Date {}/{}/{}, run {}".format(today.month,today.day,today.year,run))
        return run
        

  
    
def get_uuid(df,uuid_col): 
    
    """
    Desc : Generate uuid column
    Arguments : df: dataframe -> pyspark dataframe
                uuid_col : column name -> string
    
    Output : source_store : uuid column
    
    """
    
    split_col = F.split(df[uuid_col], '_')
    if uuid_col == 'base_source_store':
        
        uuid = F.concat(F.col('base_upc').cast("string"),F.lit('<>'),F.col('base_sku').cast("string"),F.lit('<>'),\
                    split_col.getItem(F.size(split_col) - 2),F.lit('<>'),split_col.getItem(F.size(split_col) - 1))
                    
    elif uuid_col == 'comp_source_store':
        uuid = F.concat(F.col('comp_upc').cast("string"),F.lit('<>'),F.col('comp_sku').cast("string"),F.lit('<>'),\
                    split_col.getItem(F.size(split_col) - 2),F.lit('<>'),split_col.getItem(F.size(split_col) - 1))
    return uuid  

def gen_uuid_columns(df):
    
    """
    Desc : Generate base uuid and comp uuid column
    Arguments : df: dataframe -> pyspark dataframe
    
    Output : df: dataframe with base uuid and comp uuid column -> pyspark dataframe
    
    """
    uuid_a = get_uuid(df,'base_source_store')
    df = df.withColumn('uuid_a',uuid_a)
    uuid_b = get_uuid(df,'comp_source_store')
    df = df.withColumn('uuid_b',uuid_b)
    
    return df
    
def get_store(df,uuid_col):
    
    """
    Desc : Generate store column
    Arguments : df: dataframe -> pyspark dataframe
                store_col : column name -> string
    
    Output : source_store : source store column
    
    """
    
    split_col = F.split(df[uuid_col], '<>')
    source_store = F.concat(split_col.getItem(2),F.lit('<>'),\
                    split_col.getItem(3))
    return source_store

def gen_store_columns(df):
    
    """
    Desc : Generate base store and comp store column
    Arguments : df: dataframe -> pyspark dataframe
    
    Output : df: dataframe with base store and comp store column -> pyspark dataframe
    
    """
    
    base_source_store = get_store(df,'uuid_a')
    df = df.withColumn("base_source_store",base_source_store)
    comp_source_store = get_store(df,'uuid_b')
    df = df.withColumn("comp_source_store",comp_source_store)

    return df
    
        
def get_join_uuid(df,col):
    
    """
    Desc : Generate join uuid column which would be used as key during joins
    Arguments : split_col - > split a column string on a delimeter
    
    Output : uuid : uuid column
    
    """
    
    df_uuid_norm = df.withColumn('normalized_uuid', F.regexp_replace(col, r'^[0]*', ''))
    
    #split_col = F.split(df_uuid_norm['normalized_uuid'], '<>')
    split_col = F.split(F.lower(df_uuid_norm['normalized_uuid']), '<>')
    
    invalid_fields= ['', '-', '_', '.', 'null', 'none','all','undefined','●','?','¿','0','1','na','n/a','nan']
    
    cond1 = split_col.getItem(1).isin(invalid_fields)
    cond2 = split_col.getItem(0).isin(invalid_fields)

    #cond1 = split_col.getItem(1)==''
    #cond2 = split_col.getItem(0)==''
    
    sol1 = F.concat(split_col.getItem(0),F.lit('<>'),\
                    split_col.getItem(2),F.lit('<>'),\
                    split_col.getItem(3))
                    
    sol2 = F.concat(split_col.getItem(1),F.lit('<>'),\
                    split_col.getItem(2),F.lit('<>'),\
                    split_col.getItem(3))
                    
    if domain == 'pets':
        uuid = F.when(cond1, sol1).when(cond2, sol2).otherwise(sol2)
    else:
        uuid = F.when(cond1, sol1).when(cond2, sol2).otherwise(sol1)
    
    return uuid,df_uuid_norm
    

def gen_auxillary_columns(df):
    
    """
    Desc : Generate base join uuid and comp join uuid column
    Arguments : df: dataframe -> pyspark dataframe
    
    Output : df: dataframe with base join uuid and comp join uuid column -> pyspark dataframe
    
    """
    
    join_uuid_a,df_uuid_norm = get_join_uuid(df,'uuid_a')
    df_uuid_norm = df_uuid_norm.withColumn("sku_uuid_a", join_uuid_a).drop('normalized_uuid')
    df_uuid_norm = df_uuid_norm.withColumn('sku_uuid_a', F.lower(F.col('sku_uuid_a')))
    
    join_uuid_b,df_uuid_norm = get_join_uuid(df_uuid_norm, 'uuid_b')
    df_uuid_norm = df_uuid_norm.withColumn("sku_uuid_b", join_uuid_b).drop('normalized_uuid')
    df_uuid_norm = df_uuid_norm.withColumn('sku_uuid_b', F.lower(F.col('sku_uuid_b')))
    
    
    return df_uuid_norm


    
def gen_df_from_table(database,table_name,push_down_predicate,bucket=None,prefix=None):
    
    """
    Desc : Generate dataframe from table
    Arguments : database: database name -> string 
                table_name: table_name -> string
                push_down_predicate : partition keys -> string or None
    
    Output : pairs_df: pyspark dataframe
    
    """
    
    
    if push_down_predicate:
        if table_name == 'transitive_matches':
            pairs_dyf = glueContext.create_dynamic_frame.from_catalog(database = "ml_stack_prod_matches", \
                        connection_options= {'paths': ["s3://ml-stack.prod/type=transitive_pairs/"],"useS3ListImplementation":True, 'recurse':True, 'groupFiles': 'inPartition'}, \
                        table_name = "transitive_matches", transformation_ctx = "datasource0", \
                        push_down_predicate=push_down_predicate)
            
        elif table_name == 'upc_matches':
            pairs_dyf = glueContext.create_dynamic_frame.from_catalog(database = "ml_stack_prod_matches", \
                        connection_options= {'paths': ["s3://ml-stack.prod/type=upc_matches/"],"useS3ListImplementation":True, 'recurse':True, 'groupFiles': 'inPartition'}, \
                        table_name = "upc_matches", transformation_ctx = "datasource0", \
                        push_down_predicate=push_down_predicate)
            
        else:
            pairs_dyf = glueContext.create_dynamic_frame.from_catalog(
                         database=database,
                         table_name=table_name,
                         push_down_predicate=push_down_predicate)
                         
            if table_name == 'match_library_snapshot':
                pairs_dyf = pairs_dyf.resolveChoice(specs = [('score','cast:String'),("match_date", "cast:int"),("comp_size", "cast:String"),("base_size", "cast:String"),('base_sku','cast:String'),('comp_sku','cast:String'),('base_upc','cast:String'),('comp_upc','cast:String')])
            
                        
                         
    elif bucket == None and prefix == None:
        
        pairs_dyf = glueContext.create_dynamic_frame.from_catalog(
                         database=database,
                         table_name=table_name)
                         
        if table_name == "fastlane_reviews_2021":
            pairs_dyf = pairs_dyf.resolveChoice(specs = [('score','cast:String'),("match_date", "cast:int"),("comp_size", "cast:String"),("base_size", "cast:String"),('base_sku','cast:String'),('comp_sku','cast:String'),('base_upc','cast:String'),('comp_upc','cast:String')])
        
        
        
    elif table_name == "fastlane_successful":
                         
        pairs_dyf = glueContext.create_dynamic_frame.from_options("s3", 
                                {'paths':  ["s3://bungee.productmatching/fastlane_reviews/production/successful/"],"useS3ListImplementation":True,'recurse':True, 'groupFiles': 'inPartition'}, 
                                format="json")
                                
        pairs_dyf = pairs_dyf.resolveChoice(specs = [('score','cast:String'),("match_date", "cast:int"),("comp_size", "cast:String"),("base_size", "cast:String"),('base_sku','cast:String'),('comp_sku','cast:String'),('base_upc','cast:String'),('comp_upc','cast:String')])  
        
        
        
    elif table_name == "fastlane_unsuccessful":
        
        pairs_dyf = glueContext.create_dynamic_frame.from_options("s3", 
                                {'paths':  ["s3://bungee.productmatching/fastlane_reviews/production/unsuccessful/"],"useS3ListImplementation":True,'recurse':True, 'groupFiles': 'inPartition'}, 
                                format="json")
                                
        pairs_dyf = pairs_dyf.resolveChoice(specs = [('score','cast:String'),("match_date", "cast:int"),("comp_size", "cast:String"),("base_size", "cast:String"),('base_sku','cast:String'),('comp_sku','cast:String'),('base_upc','cast:String'),('comp_upc','cast:String')])  
                         
    else:
        # pairs_dyf = glueContext.create_dynamic_frame.from_catalog(
        #                  database=database,
        #                  table_name=table_name,
        #                  additional_options={'paths': ["s3://bungee.productmatching/fastlane_reviews/production/"], "useS3ListImplementation":True,
        #                         'recurse':True, 'groupFiles': "none"}).coalesce(1048)
                                
        
                         
                    
        pairs_dyf = glueContext.create_dynamic_frame.from_options("s3", 
                                {'paths':  ["s3://bungee.productmatching/fastlane_reviews/production/year=2022/"],"useS3ListImplementation":True,'recurse':True, 'groupFiles': 'inPartition'}, 
                                format="json")
                                
        pairs_dyf = pairs_dyf.resolveChoice(specs = [('score','cast:String'),("match_date", "cast:int"),("comp_size", "cast:String"),("base_size", "cast:String"),('base_sku','cast:String'),('comp_sku','cast:String'),('base_upc','cast:String'),('comp_upc','cast:String')])

                     
    pairs_df = pairs_dyf.repartition(numOfPartitions).toDF()
    
    return pairs_df
    
    
def generate_dataframe(database,table_name,bucket,prefix,type_bucket = None,base_source_store=None):
    
    """
    Desc : Generate dataframe 
    Arguments : database: database name -> string 
                table_name: table_name -> string
                bucket: bucket name -> string
                prefix : prefix name -> string
                type_bucket : for different data partition -> string
                base_source_store : base source stores separted by '|' -> string 
    
    Output : pairs_df: pyspark dataframe
    
    """
    
    if not type_bucket:
        date = get_latest_file_date(bucket,prefix)
        if date:
            year,month,day = date
            print("Fetch Inference Pairs",date)
            if base_source_store:
                base_source_store_tup = tuple(base_source_store)
                if len(base_source_store_tup) == 1:
                    push_down_predicate="domain='{}' and year= {} and month = {} and day = {} and base_source_store = '{}'".format(domain,year,month,day,base_source_store_tup[0])
                else:
                    push_down_predicate="domain='{}' and year= {} and month = {} and day = {} and base_source_store IN {}".format(domain,year,month,day,base_source_store_tup)
                    
            else:   
                push_down_predicate="domain='{}' and year= {} and month = {} and day = {}".format(domain,year,month,day)
            print('push_down_predicate',database,table_name,push_down_predicate)
            pairs_df = gen_df_from_table(database,table_name,push_down_predicate)
            
            return pairs_df
        else:
            raise Exception('Inference Pairs Not Found At Source')
            
    elif type_bucket == 'match_lib':
        date = get_latest_file_date(bucket,prefix,'match_lib')
        if date:
            print("Fetch Inference Pairs",date,database,table_name)
            push_down_predicate="load_date = '{}'".format(date)
            print('push_down_predicate',push_down_predicate)
            pairs_df = gen_df_from_table(database,table_name,push_down_predicate)
            print(pairs_df.count())
            return pairs_df
        else:
            raise Exception('Inference Pairs Not Found At Source')
            
    elif type_bucket == 'inf_pairs':
        date = get_latest_file_date(bucket,prefix,'inf_pairs')
        if date:
            year,month,day,run = date
            
            print("Fetch Inference Pairs",date)
            if base_source_store:
                base_source_store_tup = tuple(base_source_store)
                if len(base_source_store_tup) == 1:
                    push_down_predicate="domain='{}' and year= {} and month = {} and day = {} and run_no = {} and base_source_store = '{}'".format(domain,year,month,day,run,base_source_store_tup[0])
                else:
                    push_down_predicate="domain='{}' and year= {} and month = {} and day = {} and run_no = {} and base_source_store IN {}".format(domain,year,month,day,run,base_source_store_tup)
                print('push_down_predicate',push_down_predicate)
            else:   
                push_down_predicate="domain='{}' and year= {} and month = {} and day = {}".format(domain,year,month,day)
            pairs_df = gen_df_from_table(database,table_name,push_down_predicate)
            return pairs_df
        else:
            raise Exception('Inference Pairs Not Found At Source')

     
def set_match_unix_timestamp(df):
    
    df = df.withColumn('match_date',F.concat_ws('-',df.day,df.month,df.year))
    df = df.withColumn("match_timestamp",F.unix_timestamp(df.match_date,'dd-MM-yyyy'))
    return df
    
def set_match_source(df,source): 
    
    df = df.withColumn("match_source",lit(source))
    return df
    
def set_match_type(df,match_type): 
    
    df = df.withColumn("type",lit(match_type))
    return df
    
def set_segment_type(df,segment_type): 
    
    df = df.withColumn("segment",lit(segment_type))
    return df
    
    
#deleting same day data if exists in the output folder    
outputbucket = 'ml-stack.prod' 
outputprefix = 'type=match_data_warehouse_v2'
run = gen_run_number(outputbucket,outputprefix + '/domain={}'.format(domain),today)

    
#-------------------------------------------------------------------------------------------------------------------------------

#------------------------------------Stage 1 : Fetch Fastlame Match From Athena [Extract] -----------------------------------------   

try:
    database = "ml_stack_prod_matches"
    table_name_2 = "fastlane"
    table_name_1 = "fastlane_reviews_2021"
    table_name_3 = "fastlane_successful"
    table_name_4 = "fastlane_unsuccessful"
    send_message_to_slack('{}:\n {} \n Domain : {} \n Run Type : {}'.format('Match Data Warehouse Glue Job', 'Starting', domain, run_type))
    #push_down_predicate = "workflow='{}' and year= {} and month = {} and day = {}".format('fastlane_queue_1','2021','11','10')
    print('fetching fastlane reviews')
    fastlane_df_1 = gen_df_from_table(database,table_name_1,push_down_predicate=None,bucket=None,prefix=None).drop("year","month","day")
    #print('fastlane_df_1',fastlane_df_1.count())
    # print(fastlane_df_1.show())
    print('fastlane_df_1 fetched')
    fastlane_df_2 = gen_df_from_table(database,table_name_2,None,bucket="bungee.productmatching",prefix="fastlane_reviews/production")
    print('fastlane_df_2 fetched',fastlane_df_2.count())
    #print('fastlane_df_2',fastlane_df_2.count())
    fastlane_df_3 = gen_df_from_table(database,table_name_3,None,bucket="bungee.productmatching",prefix="fastlane_reviews/production/successful")
    print('fastlane_df_3 fetched')
    fastlane_df_4 = gen_df_from_table(database,table_name_4,None,bucket="bungee.productmatching",prefix="fastlane_reviews/production/unsuccessful")
    print('fastlane_df_4 fetched')
    print('fetching fastlane reviews complete')

    # print('fastlane_df_3 successful matches',fastlane_df_3.count())    
    # print('fastlane_df_4 unsuccessful matches',fastlane_df_4.count())
    
    columnstoSelect = ['base_upc','base_sku','comp_upc','comp_sku','base_source_store','comp_source_store','score','match_date','queue_name','answer','workflow']
    fastlane_df_1 = fastlane_df_1.select(columnstoSelect)
    fastlane_df_2 = fastlane_df_2.select(columnstoSelect)
    fastlane_df_3 = fastlane_df_3.select(columnstoSelect)
    fastlane_df_4 = fastlane_df_4.select(columnstoSelect)    
    
    fastlane_df = fastlane_df_1.union(fastlane_df_2).union(fastlane_df_3).union(fastlane_df_4)
    fastlane_df = fastlane_df.fillna('', subset=['base_sku','base_source_store','base_upc','comp_sku','comp_source_store','comp_upc'])

    
    fastlane_df = fastlane_df.filter(~fastlane_df.workflow.isin(["internal-auditor","fastlane"]))
    #print("ml_stack_prod_matches :: fastlane pairs df",fastlane_df.count())
    #print(fastlane_df.show(5))
   
    #print("currentday matches")
    #temp_table = fastlane_df.filter((fastlane_df.day == '16') &(fastlane_df.month == '08'))
    #print(temp_table.show(truncate=False))

except Exception as e:
    msg = "Error Fetching Directed Pairs From Source"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)
        

# -------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------

#------------------------------------Stage 2 : Fetch Exact Desc Matches From Athena [Extract] --------------------------------------  

try:
    print('fetching Exact Desc Matches')
    bucket = outputbucket
    prefix = 'type=exact_match_desc_csv/domain={}'.format(domain)
    database="ml_stack_prod_matches"
    table_name="exact_desc_matches"
    exact_desc_matches_df = gen_df_from_table(database,table_name,push_down_predicate=None,bucket=None,prefix=None).dropDuplicates(['uuid_a', 'uuid_b'])
    exact_desc_matches_df = exact_desc_matches_df.select(F.least(exact_desc_matches_df.uuid_a,exact_desc_matches_df.uuid_b).alias('uuid_a'),\
                    F.greatest(exact_desc_matches_df.uuid_a,exact_desc_matches_df.uuid_b).alias('uuid_b'),\
                    exact_desc_matches_df.day,exact_desc_matches_df.month,exact_desc_matches_df.year,\
                    exact_desc_matches_df.score,exact_desc_matches_df.domain)
    exact_desc_matches_df = exact_desc_matches_df.dropDuplicates(['uuid_a', 'uuid_b'])
    print('fetching Exact Desc Matches complete')
    #transitive_df = generate_dataframe(database,table_name,bucket,prefix,type_bucket = None,base_source_store = None)
    #print("Printing Exact Match Descriptor Pairs")
    #print(exact_desc_matches_df.show())

except Exception as e:
    msg = "Error Fetching Exact Match Descriptor Pairs From Source"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)


# #-------------------------------------------------------------------------------------------------------------------------


#------------------------------------Stage 3 : Fetch Transitive Matches From Athena [Extract] --------------------------------------  

try:
    print('fetching transitive matches')
    bucket = outputbucket
    prefix = 'type=transitive_pairs/domain={}'.format(domain)
    database="ml_stack_prod_matches"
    table_name="transitive_matches"
    #transitive_df = gen_df_from_table(database,table_name,None,bucket,prefix).dropDuplicates(['uuid_a', 'uuid_b'])
    transitive_df_pets = gen_df_from_table(database,table_name,"domain='pets'",bucket=None,prefix=None)
    print('pets fetched')
    transitive_df_office_supplies = gen_df_from_table(database,table_name,"domain='office_supplies'",bucket=None,prefix=None)
    #print(transitive_df_office_supplies.show(5))
    print('staples fetched')
    transitive_df_grocery = gen_df_from_table(database,table_name,"domain='grocery'",bucket=None,prefix=None)
    print('grocery fetched')
    transitive_df = transitive_df_pets.union(transitive_df_office_supplies)
    transitive_df = transitive_df.union(transitive_df_grocery)

    #print(transitive_df.count())
    transitive_df = transitive_df.dropDuplicates(['uuid_a', 'uuid_b'])

    print('fetching transitive matches complete')


    #print(transitive_df.show(5))

    transitive_df = transitive_df.select(F.least(transitive_df.uuid_a,transitive_df.uuid_b).alias('uuid_a'),\
                    F.greatest(transitive_df.uuid_a,transitive_df.uuid_b).alias('uuid_b'),\
                    transitive_df.day,transitive_df.month,transitive_df.year,\
                    transitive_df.scores,transitive_df.domain)
    transitive_df = transitive_df.dropDuplicates(['uuid_a', 'uuid_b'])
    
    #transitive_df = generate_dataframe(database,table_name,bucket,prefix,type_bucket = None,base_source_store = None)
    

except Exception as e:
    msg = "Error Fetching Transitive Match Pairs From Source"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)



#------------------------------------Stage 4 : Fetch UPC Match From Athena [Extract] --------------------------------------  
#if run_type == 'weekly':
try:
    print('fetching UPC matches')
    bucket = outputbucket
    prefix = 'type=upc_matches/domain={}'.format(domain)
    database="ml_stack_prod_matches"
    table_name="upc_matches"
    #upc_df = generate_dataframe(database,table_name,bucket,prefix,type_bucket = None,base_source_store = None).dropDuplicates(['uuid_a', 'uuid_b'])
    #upc_df = gen_df_from_table(database,table_name,None,bucket,prefix).dropDuplicates(['uuid_a', 'uuid_b'])
    upc_df_pets = gen_df_from_table(database,table_name,"domain='pets'",bucket=None,prefix=None)
    print('pets fetched')
    upc_df_office_supplies = gen_df_from_table(database,table_name,"domain='office_supplies'",bucket=None,prefix=None)
    print('staples fetched')
    upc_df_grocery = gen_df_from_table(database,table_name,"domain='grocery'",bucket=None,prefix=None)
    print('grocery fetched')
    upc_df = upc_df_pets.union(upc_df_office_supplies)
    upc_df = upc_df.union(upc_df_grocery)
    upc_df = upc_df.dropDuplicates(['uuid_a', 'uuid_b'])
    print('fetching UPC matches complete')
    
    #print("Printing UPC Pairs",upc_df.count())
    #print(upc_df.show(5))
    upc_df = upc_df.select(F.least(upc_df.uuid_a,upc_df.uuid_b).alias('uuid_a'),
                    F.greatest(upc_df.uuid_a,upc_df.uuid_b).alias('uuid_b'),upc_df.day,upc_df.month,upc_df.year,upc_df.score,upc_df.domain)
    upc_df = upc_df.dropDuplicates(['uuid_a', 'uuid_b'])
    
    #print("Printing UPC Pairs After")
    #print(upc_df.show())

except Exception as e:
    msg = "Error Fetching UPC Match Pairs From Source"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)


#-------------------------------------------------------------------------------------------------------------------------
    

#------------------------------------Stage 5 : Fetch Match Pairs From Athena [Extract] --------------------------------------  

try:
    bucket = 'data.bungineering'
    prefix = 'matches_backup/data/new_full_backup'
    database='match_library' 
    table_name='match_library_snapshot'
    ml_df = generate_dataframe(database,table_name,bucket,prefix,type_bucket = 'match_lib',base_source_store = None)
    print('match library fetched')
    columnstoSelect = ['base_upc','base_source_store','comp_source_store','base_sku','company_code','comp_upc', \
                 'match_status','comp_sku','customer_review_state','deleted_date','match','active','model_used','score','match_date']
    ml_df = ml_df.filter(ml_df.company_code != "agg_ml")
    ml_df = ml_df.select(columnstoSelect)
    ml_df = ml_df.fillna('', subset=['base_sku','base_source_store','base_upc','comp_sku','comp_source_store','comp_upc'])

    #print("Printing Matches")
    #print(ml_df.show())

except Exception as e:
    msg = "Error Fetching Match Pairs From Match Library"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)

# #-------------------------------------------------------------------------------------------------------------------------


# #------------------------------------Stage 6 : Generate Auxuliary Columns Match Data [Transformation] --------------------------------------  

try:
    print("Generating Auxuliary Columns Match Data")

    ml_stage_df = gen_uuid_columns(ml_df)

    #print("test 4")
    #print(ml_stage_df.where((ml_stage_df.base_sku == '156563') & (ml_stage_df.base_source_store == 'chewy_chewy')  & (ml_stage_df.comp_source_store == 'jefferspet_jefferspet')).show())

    ml_stage_df = gen_auxillary_columns(ml_stage_df).drop('base_source_store','comp_source_store')
    #print("Matches")
    #print(ml_stage_df.show(truncate=False))

    ml_stage_df = gen_store_columns(ml_stage_df)
    #match_stage_df = ml_stage_df.filter((ml_stage_df.match_status == 'product_found') & (ml_stage_df.customer_review_state.isin(['both_customer_manager_verified','customer_verified']))
    #                        & ((ml_stage_df.deleted_date == '') | ml_stage_df.deleted_date.isNull()))

    #print("test 5")
    #print(ml_stage_df.where((ml_stage_df.base_sku == '156563') & (ml_stage_df.base_source_store == 'chewy<>chewy')  & (ml_stage_df.comp_source_store == 'jefferspet_jefferspet')).show())

    #print("test 1")
    #print(ml_stage_df.where((ml_stage_df.base_sku == '65070') & (ml_stage_df.base_source_store == 'chewy<>chewy')).show(truncate = False))

    #print("test 1")
    #print(ml_stage_df.where((ml_stage_df.base_sku == '129044') & (ml_stage_df.base_source_store == 'chewy<>chewy')  & (ml_stage_df.comp_source_store == 'groomerschoice<>groomerschoice')).show(truncate=False))


    match_stage_df = ml_stage_df.filter((ml_stage_df.match_status == 'product_found') & ((ml_stage_df.deleted_date.isNull()) | (ml_stage_df.deleted_date == '')))
    not_match_stage_df = ml_stage_df.filter((ml_stage_df.deleted_date.isNotNull()) | (ml_stage_df.deleted_date != ''))

    not_matches_df = set_match_type(not_match_stage_df,'not_a_match')
    not_matches_df = not_matches_df.withColumn('model_used', F.lower(not_matches_df.model_used))
    not_matches_pets = set_segment_type(not_matches_df.filter(not_matches_df.company_code.isin(['chewy','petco'])),'pets')
    not_matches_grocery = set_segment_type(not_matches_df.filter(not_matches_df.company_code.isin(['wholefoods','farmstead','imperfect'])),'grocery')
    not_matches_office_supplies = set_segment_type(not_matches_df.filter(not_matches_df.company_code.isin(['staples'])),'office_supplies')

    not_matches_df = not_matches_office_supplies.union(not_matches_grocery)
    not_matches_df = not_matches_df.union(not_matches_pets)

    not_matches_manual_df = set_match_source(not_matches_df.filter(not_matches_df.model_used.contains('manual')),'manual')
    not_matches_fastlane_df = set_match_source(not_matches_df.filter((not_matches_df.model_used.contains('fastlane') | not_matches_df.model_used.contains('ml'))),'fastlane_customer_scrape_rejected')
    not_matches_upc_df = set_match_source(not_matches_df.filter(not_matches_df.model_used.contains('upc')),'upc')
    not_matches_transitive_df = set_match_source(not_matches_df.filter(not_matches_df.model_used.contains('transitive')),'transitive')

    not_matches_df = not_matches_manual_df.union(not_matches_fastlane_df)
    not_matches_df = not_matches_df.union(not_matches_upc_df)
    not_matches_df = not_matches_df.union(not_matches_transitive_df)

    not_matches_df = not_matches_df.withColumnRenamed("match_date","match_timestamp").alias('not_matches_df')
    #print('not_matches_df',not_matches_df.count())
    #print(not_matches_df.show())


    #Filter Pets Matches
    chewy_legacy_matches_df = match_stage_df.filter((match_stage_df.company_code.isin(['chewy','petco'])) & (match_stage_df.model_used.isin(['ManualInternal','ManualExternal','MLT'])))
        
        
    chewy_manual_matches_df = match_stage_df.filter((match_stage_df.company_code.isin(['chewy','petco'])) & (match_stage_df.model_used.contains('Manual-')))
        

    chewy_legacy_exact_matches_df = set_match_type(chewy_legacy_matches_df.filter(chewy_legacy_matches_df.match == 'exact'),'exact_match')
    chewy_legacy_similar_matches_df = set_match_type(chewy_legacy_matches_df.filter(chewy_legacy_matches_df.match == 'similar'),'similar_match')


    chewy_manual_exact_matches_df = set_match_type(chewy_manual_matches_df.filter(chewy_manual_matches_df.match == 'exact'),'exact_match')
    chewy_manual_similar_matches_df = set_match_type(chewy_manual_matches_df.filter(chewy_manual_matches_df.match == 'similar'),'similar_match')


    chewy_manual_matches_df = chewy_manual_exact_matches_df.union(chewy_manual_similar_matches_df)
    chewy_manual_matches_df = set_match_source(chewy_manual_matches_df,'manual')

    chewy_legacy_matches_df = chewy_legacy_exact_matches_df.union(chewy_legacy_similar_matches_df)
    chewy_legacy_matches_df = set_match_source(chewy_legacy_matches_df,'legacy')

    chewy_sim_engine_matches = match_stage_df.filter((match_stage_df.company_code.isin(['chewy','petco'])) & (match_stage_df.model_used.isin(['SimEngineAutov1','SimEnginev1'])))
    chewy_sim_engine_matches = set_match_type(chewy_sim_engine_matches.filter(chewy_sim_engine_matches.match == 'exact'),'exact_match')
    chewy_sim_engine_matches = set_match_source(chewy_sim_engine_matches,'legacy_ml')
    chewy_matches_df = chewy_legacy_matches_df.union(chewy_sim_engine_matches)
    chewy_matches_df = chewy_matches_df.union(chewy_manual_matches_df)

    chewy_fastlane_matches = match_stage_df.filter((match_stage_df.company_code.isin(['chewy','petco'])) & (match_stage_df.model_used.isin(['ml_v1.0','ml','btfastlane-ml',\
    'btfastlane-ml_v1.0','btfastlaneMock-ml'])))
    chewy_fastlane_matches = set_match_type(chewy_fastlane_matches.filter(chewy_fastlane_matches.match == 'exact'),'exact_match')
    chewy_fastlane_matches = set_match_source(chewy_fastlane_matches,'fastlane')
    chewy_matches_df = chewy_matches_df.union(chewy_fastlane_matches)


    chewy_upc_matches = match_stage_df.filter((match_stage_df.company_code.isin(['chewy','petco'])) & (match_stage_df.model_used.isin(['UPCMatch','upc','btfastlane-upc'])))
    chewy_upc_matches = set_match_type(chewy_upc_matches.filter(chewy_upc_matches.match == 'exact'),'exact_match')
    chewy_upc_matches = set_match_source(chewy_upc_matches,'upc_legacy')
    chewy_matches_df = chewy_matches_df.union(chewy_upc_matches)

    btfastlane_matches_df = match_stage_df.filter((match_stage_df.company_code == 'btfastlane') & (match_stage_df.customer_review_state.isin(['both_customer_manager_verified','customer_verified'])))
    btfastlane_matches_df = set_match_type(btfastlane_matches_df.filter(btfastlane_matches_df.match == 'exact'),'exact_match')
    btfastlane_matches_df = set_match_source(btfastlane_matches_df,'fastlane')
    pets_matches_df = chewy_matches_df.union(btfastlane_matches_df)
    pets_matches_df = set_segment_type(pets_matches_df,'pets')
    pets_matches_df = pets_matches_df.withColumnRenamed("match_date","match_timestamp")

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    pets_matches_df = pets_matches_df.select(columnstoSelect)
    not_matches_df = not_matches_df.select(columnstoSelect)

    #grocery matches
    grocery_matches_df = match_stage_df.filter((match_stage_df.company_code.isin(['wholefoods','farmstead','imperfect'])) & ((match_stage_df.comp_sku.isNotNull()) | (match_stage_df.comp_sku != '')))
    grocery_matches_df = set_match_type(grocery_matches_df.filter(grocery_matches_df.match == 'exact'),'exact_match')
    grocery_matches_df = set_match_source(grocery_matches_df,'legacy')
    grocery_matches_df = set_segment_type(grocery_matches_df,'grocery')
    grocery_matches_df = grocery_matches_df.withColumnRenamed("match_date","match_timestamp")

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    grocery_matches_df = grocery_matches_df.select(columnstoSelect)


    #office_supplies matches
    office_supplies_matches_df = match_stage_df.filter((match_stage_df.company_code.isin(['staples'])) & ((match_stage_df.comp_sku.isNotNull()) | (match_stage_df.comp_sku != '')))

    office_supplies_exact_matches_df = set_match_type(office_supplies_matches_df.filter(office_supplies_matches_df.match == 'exact'),'exact_match')
    office_supplies_similar_matches_df = set_match_type(office_supplies_matches_df.filter(office_supplies_matches_df.match == 'equivalent'),'similar_match')

    office_supplies_matches_df = office_supplies_exact_matches_df.union(office_supplies_similar_matches_df)

    office_supplies_matches_manual_df = office_supplies_matches_df.filter(office_supplies_matches_df.model_used.contains('Manual'))
    office_supplies_matches_manual_df = set_match_source(office_supplies_matches_manual_df,'manual')
    office_supplies_matches_fastlane_df = office_supplies_matches_df.filter(office_supplies_matches_df.model_used.contains('fastlane'))
    office_supplies_matches_fastlane_df = set_match_source(office_supplies_matches_fastlane_df,'fastlane')

    office_supplies_matches_df = office_supplies_matches_fastlane_df.union(office_supplies_matches_manual_df)

    office_supplies_matches_df = set_segment_type(office_supplies_matches_df,'office_supplies')
    office_supplies_matches_df = office_supplies_matches_df.withColumnRenamed("match_date","match_timestamp")

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    office_supplies_matches_df = office_supplies_matches_df.select(columnstoSelect)

    match_library_matches_df = grocery_matches_df.union(pets_matches_df)
    match_library_matches_df = match_library_matches_df.union(office_supplies_matches_df)

    #print(match_library_matches_df.show(n=5))


except Exception as e:
    msg = "Error Generating Auxuliary Columns Match Data"
    #send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)

#-------------------------------------------------------------------------------------------------------------------------


#------------------------------------Stage 8 : Generate Auxuliary Columns Fastlane Data [Transformation] --------------------------------------  


try:
    print("Generating Auxuliary Columns Match Data")

    fastlane_stage_df = gen_uuid_columns(fastlane_df)



    fastlane_stage_df = gen_auxillary_columns(fastlane_stage_df)#.drop('base_source_store','comp_source_store')
    # temp_df_1 = fastlane_stage_df.filter((fastlane_stage_df.sku_uuid_a == '292700<>chewy<>chewy') & ((fastlane_stage_df.sku_uuid_b == '') | (fastlane_stage_df.sku_uuid_b.isNull())))

    # print('temp_df3')
    # print(temp_df_1.show())
    fastlane_stage_df = gen_store_columns(fastlane_stage_df)
    #match_stage_df = ml_stage_df.filter((ml_stage_df.match_status == 'product_found') & (ml_stage_df.customer_review_state.isin(['both_customer_manager_verified','customer_verified']))
    #                        & ((ml_stage_df.deleted_date == '') | (ml_stage_df.deleted_date == None)) & (ml_stage_df.match == 'exact'))

    # temp_df = fastlane_stage_df.filter((fastlane_stage_df.sku_uuid_a.isin(['292700<>chewy<>chewy','297836<>chewy<>chewy','189814<>chewy<>chewy','320156<>chewy<>chewy'])) | (fastlane_stage_df.sku_uuid_b.isin(['292700<>chewy<>chewy','297836<>chewy<>chewy','189814<>chewy<>chewy','320156<>chewy<>chewy'])))


    # temp_df_1 = fastlane_stage_df.filter((fastlane_stage_df.sku_uuid_a == '292700<>chewy<>chewy') & ((fastlane_stage_df.sku_uuid_b == '') | (fastlane_stage_df.sku_uuid_b.isNull())))

    # temp_df_2 = fastlane_stage_df.filter((fastlane_stage_df.sku_uuid_b == '292700<>chewy<>chewy') & ((fastlane_stage_df.sku_uuid_a == '') | (fastlane_stage_df.sku_uuid_a.isNull())))

    # print('temp_df1')
    # print(temp_df_1.show())

    # print('temp_df2')
    # print(temp_df_2.show())

    exact_match_flags = ['exact_match','exact_match_dq_issue','exact_match_with_image']
    not_match_flags = ['not_a_match','not_match_image','not_match_dq_issue']
    fastlane_match_stage_df = fastlane_stage_df.filter(fastlane_stage_df.answer.isin(exact_match_flags))
    fastlane_not_match_stage_df = fastlane_stage_df.filter(fastlane_stage_df.answer.isin(not_match_flags))
    fastlane_sim_match_stage_df = fastlane_stage_df.filter(~fastlane_stage_df.answer.isin(exact_match_flags+not_match_flags+['unsure']))

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','queue_name','comp_source_store','match_date']

    fin_fastlane_match_df = fastlane_match_stage_df.select(columnstoSelect).alias('fin_fastlane_match_df') 
    fin_fastlane_match_df = fin_fastlane_match_df.withColumn("type",lit("exact_match"))


    fin_fastlane_not_match_df = fastlane_not_match_stage_df.select(columnstoSelect).alias('fin_fastlane_not_match_df')
    fin_fastlane_not_match_df = fin_fastlane_not_match_df.withColumn("type",lit("not_a_match"))

    fin_fastlane_sim_match_df = fastlane_sim_match_stage_df.select(columnstoSelect).alias('fin_fastlane_sim_match_df')
    fin_fastlane_sim_match_df = fin_fastlane_sim_match_df.withColumn("type",lit("similar_match"))

    fin_fastlane_match_df = fin_fastlane_match_df.select(columnstoSelect+['type']).alias('fin_fastlane_match_df') 
    fin_fastlane_not_match_df = fin_fastlane_not_match_df.select(columnstoSelect+['type']).alias('fin_fastlane_not_match_df')
    fin_fastlane_sim_match_df = fin_fastlane_sim_match_df.select(columnstoSelect+['type']).alias('fin_fastlane_sim_match_df')

    fin_fastlane_df = fin_fastlane_match_df.union(fin_fastlane_sim_match_df)
    fin_fastlane_df = fin_fastlane_df.union(fin_fastlane_not_match_df)


    fin_fastlane_pets_df = fin_fastlane_df.filter(~((fin_fastlane_df.queue_name.contains('grocery')) | (fin_fastlane_df.queue_name.contains('staples'))))
    fin_fastlane_pets_df = set_segment_type(fin_fastlane_pets_df,'pets')


    fin_fastlane_grocery_df = fin_fastlane_df.filter(fin_fastlane_df.queue_name.contains('grocery'))
    fin_fastlane_grocery_df = set_segment_type(fin_fastlane_grocery_df,'grocery')

    fin_fastlane_office_supplies_df = fin_fastlane_df.filter(fin_fastlane_df.queue_name.contains('staples'))
    fin_fastlane_office_supplies_df = set_segment_type(fin_fastlane_office_supplies_df,'office_supplies')


    fin_fastlane_df = fin_fastlane_pets_df.union(fin_fastlane_grocery_df)
    fin_fastlane_df = fin_fastlane_df.union(fin_fastlane_office_supplies_df)

    fin_fastlane_df = fin_fastlane_df.withColumnRenamed("match_date","match_timestamp").withColumn("match_timestamp", F.col("match_timestamp").cast(IntegerType()))
    fin_fastlane_df = fin_fastlane_df.withColumn("match_source",lit("fastlane"))


    #fin_fastlane_df = fin_fastlane_df.withColumn('score', F.explode(F.array('score.*')))#.withColumn("score", F.col('scores').getField('_1'))
    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    fin_fastlane_match_df = fin_fastlane_df.filter(fin_fastlane_df.type.isin(['exact_match','similar_match'])).select(columnstoSelect).dropDuplicates(['sku_uuid_a', 'sku_uuid_b'])

    fin_fastlane_not_match_df = fin_fastlane_df.filter(fin_fastlane_df.type == 'not_a_match').select(columnstoSelect).dropDuplicates(['sku_uuid_a', 'sku_uuid_b'])

    # temp_df_2 = fin_fastlane_match_df.filter((fin_fastlane_match_df.sku_uuid_a.isin(['292700<>chewy<>chewy','297836<>chewy<>chewy','189814<>chewy<>chewy','320156<>chewy<>chewy'])) | (fin_fastlane_match_df.sku_uuid_b.isin(['292700<>chewy<>chewy','297836<>chewy<>chewy','189814<>chewy<>chewy','320156<>chewy<>chewy'])))

    # temp_df_3 = fin_fastlane_not_match_df.filter((fin_fastlane_not_match_df.sku_uuid_a.isin(['292700<>chewy<>chewy','297836<>chewy<>chewy','189814<>chewy<>chewy','320156<>chewy<>chewy'])) | (fin_fastlane_not_match_df.sku_uuid_b.isin(['292700<>chewy<>chewy','297836<>chewy<>chewy','189814<>chewy<>chewy','320156<>chewy<>chewy'])))

    # print('temp_df_2')
    # print(temp_df_2.show())

    # print('temp_df_3')
    # print(temp_df_3.show())

    # print("Printing Fastlane Matches")
    # print(fin_fastlane_df.show(truncate=False))
    # print("Printing Fastlane Not Matches")
    # print(fin_fastlane_not_match_df.show(truncate=False))


except Exception as e:
    msg = "Error Generating Auxuliary Columns Fastlane Data"
    #send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)

#-------------------------------------------------------------------------------------------------------------------------
                            
                            


# # #-------------------------------------------------------------------------------------------------------------------------------

#------------------------------------Stage 10 : Generate Auxuliary Columns UPC Pairs [Transformation] --------------------------------------  
#if run_type == 'weekly':
try:
    print("Generating Auxuliary Columns UPC Pairs")

    upc_stage_df = gen_auxillary_columns(upc_df)
    upc_stage_df = gen_store_columns(upc_stage_df)
    upc_stage_df = set_match_unix_timestamp(upc_stage_df)
    upc_stage_df = set_match_type(upc_stage_df,'exact_match')
    upc_stage_df = set_match_source(upc_stage_df,'upc')
    upc_stage_df = upc_stage_df.withColumnRenamed("domain","segment")

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    upc_stage_df = upc_stage_df.select(columnstoSelect).alias('upc_stage_df')


                
    #print("Printing UPC Pair Records")
    #print(upc_stage_df.show())
except Exception as e:
    msg = "Error Generating Auxuliary Columns UPC Data"
    logger.info(msg)
    logger.info(e)



# #-------------------------------------------------------------------------------------------------------------------------------

#------------------------------------Stage 10 : Generate Auxuliary Columns Exact Desc Match Pairs [Transformation] --------------------------------------  
#if run_type == 'weekly':
try:
    print("Generating Auxuliary Columns UPC Pairs")

    exact_desc_stage_df = gen_auxillary_columns(exact_desc_matches_df)
    exact_desc_stage_df = gen_store_columns(exact_desc_stage_df)
    exact_desc_stage_df = set_match_unix_timestamp(exact_desc_stage_df)
    exact_desc_stage_df = set_match_type(exact_desc_stage_df,'exact_match')
    exact_desc_stage_df = set_match_source(exact_desc_stage_df,'exact_desc_match')
    exact_desc_stage_df = exact_desc_stage_df.withColumnRenamed("domain","segment")

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    exact_desc_stage_df = exact_desc_stage_df.select(columnstoSelect).alias('exact_desc_stage_df')


                
    #print("Printing Exact Match Records")
    #print(exact_desc_stage_df.show())
except Exception as e:
    msg = "Error Generating Auxuliary Columns UPC Data"
    logger.info(msg)
    logger.info(e)


# #------------------------------------Stage 11 : Generate Auxuliary Columns Transitive Pairs [Transformation] --------------------------------------  

try:
    print("Generating Auxuliary Columns Transitive Pairs")

    transitive_stage_df = gen_auxillary_columns(transitive_df)
    transitive_stage_df = gen_store_columns(transitive_stage_df)
    transitive_stage_df = set_match_unix_timestamp(transitive_stage_df)
    transitive_stage_df = set_match_type(transitive_stage_df,'exact_match')
    transitive_stage_df = set_match_source(transitive_stage_df,'transitive')
    transitive_stage_df = transitive_stage_df.withColumnRenamed("scores","score")
    transitive_stage_df = transitive_stage_df.withColumnRenamed("domain","segment")

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    transitive_stage_df = transitive_stage_df.select(columnstoSelect).alias('transitive_stage_df')
                
    #print("Printing Transitive Pair Records")
    #print(transitive_stage_df.show())
    #print("test 1")
    #print(transitive_stage_df.where((transitive_stage_df.join_uuid_a == '206463<>chewy<>chewy') & (transitive_stage_df.comp_source_store == 'baxterboo<>baxterboo')).show(truncate = False))
    #print(transitive_stage_df.where((transitive_stage_df.join_uuid_b == '206463<>chewy<>chewy') & (transitive_stage_df.base_source_store == 'baxterboo<>baxterboo')).show(truncate = False))

except Exception as e:
    msg = "Error Generating Auxuliary Columns Transitive Data"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)



#-------------------------------------------------------------------------------------------------------------------------------


# #------------------------------------Stage 8 : Combine All Match Data [Transformation] -------------------------------------- 

try:
    print("Combining Match Data and UPC Pairs")

    fin_match_df = match_library_matches_df.union(fin_fastlane_match_df)
    fin_match_df = fin_match_df.union(transitive_stage_df)
    fin_match_df = fin_match_df.union(exact_desc_stage_df)
    fin_match_df = fin_match_df.union(upc_stage_df).dropDuplicates(['sku_uuid_a', 'sku_uuid_b']).alias('fin_match_df')
    
    fin_match_df = fin_match_df.withColumn('uuid_a', F.regexp_replace('uuid_a', 'chewyinternal', 'chewy'))
    fin_match_df = fin_match_df.withColumn('uuid_b', F.regexp_replace('uuid_b', 'chewyinternal', 'chewy'))
    fin_match_df = fin_match_df.withColumn('sku_uuid_a', F.regexp_replace('sku_uuid_a', 'chewyinternal', 'chewy'))
    fin_match_df = fin_match_df.withColumn('sku_uuid_b', F.regexp_replace('sku_uuid_b', 'chewyinternal', 'chewy'))
    fin_match_df = fin_match_df.withColumn('base_source_store', F.regexp_replace('base_source_store', 'chewyinternal', 'chewy'))
    fin_match_df = fin_match_df.withColumn('comp_source_store', F.regexp_replace('comp_source_store', 'chewyinternal', 'chewy'))
    fin_match_df = fin_match_df.dropDuplicates(['sku_uuid_a', 'sku_uuid_b']).alias('fin_match_df')

    #pruning not matches
    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']
    not_matches_df = not_matches_df.select(columnstoSelect)
    fin_fastlane_not_match_df = fin_fastlane_not_match_df.select(columnstoSelect)

    not_matches_df = not_matches_df.union(fin_fastlane_not_match_df).dropDuplicates(['sku_uuid_a', 'sku_uuid_b'])

    # fin_match_df = fin_match_df.join(not_matches_df,((F.lower(F.col('not_matches_df.sku_uuid_a')) == F.lower(F.col('fin_match_df.sku_uuid_a'))) & \
    #         (F.lower(F.col('not_matches_df.sku_uuid_b')) == F.lower(F.col('fin_match_df.sku_uuid_b')))),"leftanti").alias('fin_match_df')
    # #print("Number of Records after Pruning stage 5",prune_df_v5.count())
        
    # fin_match_df = fin_match_df.join(not_matches_df,((F.lower(F.col('fin_match_df.sku_uuid_a')) == F.lower(F.col('not_matches_df.sku_uuid_b'))) & \
    #         (F.lower(F.col('fin_match_df.sku_uuid_b')) == F.lower(F.col('not_matches_df.sku_uuid_a')))),"leftanti").alias('fin_match_df')
    
    #print("Final Match Warehouse")
    #print(fin_match_df.show())

    year,month,day = gen_partitions(today)

    not_matches_df = not_matches_df.select(columnstoSelect)
    fin_match_df = fin_match_df.select(columnstoSelect)

    fin_match_df = fin_match_df.union(not_matches_df)

    fin_match_df = fin_match_df.withColumn('year', lit(year)).withColumn('month', lit(month)).withColumn('day', lit(day))

    #print('*************************\nfin_match_df schema',fin_match_df.schema)
    dup_inf_pairs = fin_match_df.withColumn("uuid_a_temp",F.col("uuid_a")).drop("uuid_a")
    dup_inf_pairs = dup_inf_pairs.withColumn("uuid_b_temp",F.col("uuid_b")).drop("uuid_b")
    dup_inf_pairs = dup_inf_pairs.withColumnRenamed("uuid_a_temp","uuid_b")
    dup_inf_pairs = dup_inf_pairs.withColumnRenamed("uuid_b_temp","uuid_a")

    dup_inf_pairs = dup_inf_pairs.withColumn("sku_uuid_a_temp",F.col("sku_uuid_a")).drop("sku_uuid_a")
    dup_inf_pairs = dup_inf_pairs.withColumn("sku_uuid_b_temp",F.col("sku_uuid_b")).drop("sku_uuid_b")
    dup_inf_pairs = dup_inf_pairs.withColumnRenamed("sku_uuid_a_temp","sku_uuid_b")
    dup_inf_pairs = dup_inf_pairs.withColumnRenamed("sku_uuid_b_temp","sku_uuid_a")

    dup_inf_pairs = dup_inf_pairs.withColumn("base_source_store_temp",F.col("base_source_store")).drop("base_source_store")
    dup_inf_pairs = dup_inf_pairs.withColumn("comp_source_store_temp",F.col("comp_source_store")).drop("comp_source_store")
    dup_inf_pairs = dup_inf_pairs.withColumnRenamed("base_source_store_temp","comp_source_store")
    dup_inf_pairs = dup_inf_pairs.withColumnRenamed("comp_source_store_temp","base_source_store")

    columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment','year','month','day']
    dup_inf_pairs = dup_inf_pairs.select(columnstoSelect)
    fin_match_df = fin_match_df.select(columnstoSelect)
                            
    fin_df = fin_match_df.union(dup_inf_pairs)


    # fin_match_df = fin_match_df.select(F.least(fin_match_df.sku_uuid_a,fin_match_df.sku_uuid_b).alias('sku_uuid_a'), \
    #                 F.greatest(fin_match_df.sku_uuid_a,fin_match_df.sku_uuid_b).alias('sku_uuid_b'),fin_match_df.uuid_a,fin_match_df.uuid_b,\
    #                 fin_match_df.score,fin_match_df.base_source_store,fin_match_df.comp_source_store,fin_match_df.segment,\
    #                 fin_match_df.match_source,fin_match_df.type,fin_match_df.match_timestamp,\
    #                 fin_match_df.day,fin_match_df.month,fin_match_df.year)
                


    #sku_uuid_a                sku_uuid_b                  base_ss      comp_ss   uuid_a   uuid_b  
    #upc<>chewy<>chewy         2345<>amazon<>amazon        amazon       chewy     
    #-<>chewy<>chewy         2345<>amazon<>amazon        chewy        amazon 


                
    # column_list = ['sku_uuid_a', 'sku_uuid_b']
    # window = Window.partitionBy([F.col(x) for x in column_list]).orderBy("match_timestamp")
    # fin_match_df = fin_match_df.withColumn('rank', rank().over(window)).filter(F.col('rank') == 1).drop('rank') 
        
    #fin_match_df = fin_df.dropDuplicates(['sku_uuid_a', 'sku_uuid_b']).alias('fin_match_df')

    fin_match_df = fin_df.select(columnstoSelect).repartition(numOfPartitions)

    # match_df = fin_match_df.filter(fin_match_df.type.isin(['similar_match','exact_match']))
    # not_a_match_df = fin_match_df.filter(fin_match_df.type == 'not_a_match')

    #columnstoSelect = ['uuid_a','uuid_b','sku_uuid_a','sku_uuid_b','score','base_source_store','comp_source_store','match_timestamp','match_source','type','segment']


except Exception as e:
    msg = "Error Combining UPC and Match Data"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)

# # #------------------------------------Stage 10 :Inserting Records Pairs [Load] -------------------------------------- 

try:

    final_dyf = DynamicFrame.fromDF(fin_match_df, glueContext, "final_dyf")
    final_dyf = final_dyf.resolveChoice(specs = [('uuid_a','cast:string'),('uuid_b','cast:string')])

    print("Inserting Pairs Into S3")

    sinkdatabase = "ml_stack_prod_matches"
    sinktable = "match_data_warehouse_v2"

    sink = glueContext.getSink(
        enableUpdateCatalog=True,
        connection_type = "s3",    
        path = "s3://{}/{}/".format(outputbucket,outputprefix), 
        partitionKeys = ["year","month","day"])
    
    sink.setFormat("csv")
    sink.setCatalogInfo(catalogDatabase=sinkdatabase, catalogTableName=sinktable)
    sink.writeFrame(final_dyf)
    
    
    
except Exception as e:
    msg = "Error Inserting Data"
    send_message_to_slack('{}:\n {}'.format(msg, str(e)))
    logger.info(msg)
    logger.info(e)
    
    
# try:

#     final_dyf = DynamicFrame.fromDF(not_a_match_df, glueContext, "final_dyf")
#     final_dyf = final_dyf.resolveChoice(specs = [('uuid_a','cast:string'),('uuid_b','cast:string')])
    
#     print("Inserting Pairs Into S3")
    
#     sinkdatabase = "ml_stack_prod_matches"
#     sinktable = "not_a_match_data_warehouse_v2"
    
#     outputprefix = "type=not_a_match_data_warehouse_v2"
    
#     sink = glueContext.getSink(
#         enableUpdateCatalog=True,
#         connection_type = "s3",    
#         path = "s3://{}/{}/".format(outputbucket,outputprefix), 
#         partitionKeys = ["year","month","day"])
        
#     sink.setFormat("csv")
#     sink.setCatalogInfo(catalogDatabase=sinkdatabase, catalogTableName=sinktable)
#     sink.writeFrame(final_dyf)
        
        
        
# except Exception as e:
#     msg = "Error Inserting Data"
#     send_message_to_slack('{}:\n {}'.format(msg, str(e)))
#     logger.info(msg)
#     logger.info(e)    


# job.commit()    
    

# # # #-------------------------------------------------------------------------------------------------------------------------------