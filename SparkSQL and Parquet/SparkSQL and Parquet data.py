from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField
from pyspark import SparkContext
url = 'parquet_data\\Risk_2_20230628095932.parquet'

schema = StructType({StructField("CreatedDate", StringType(), True),
                     StructField("Id", StringType(), True),
                     StructField("OwnerId", StringType(), True),
                     StructField("IsDeleted", StringType(), True),
                     StructField("Name", StringType(), True),
                     StructField("RecordTypeId", StringType(), True),
                     StructField("CreatedById", StringType(), True),
                     StructField("LastModifiedDate", StringType(), True),
                     StructField("LastModifiedById", StringType(), True),
                     StructField("SystemModstamp", StringType(), True),
                     StructField("LastActivityDate", StringType(), True),
                     StructField("LastViewedDate", StringType(), True),
                     StructField("LastReferencedDate", StringType(), True),
                     StructField("rkc__AM_Best_Major_Category__c", StringType(), True),
                     StructField("rkc__AM_Best_Minor_Category__c", StringType(), True),
                     StructField("rkc__Action_Plan__c", StringType(), True),
                     StructField("rkc__Current_Impact_Decimal__c", StringType(), True),
                     StructField("rkc__Current_Impact_Number__c", StringType(), True),
                     StructField("rkc__Current_Impact__c", StringType(), True),
                     StructField("rkc__Current_Likelihood_Decimal__c", StringType(), True),
                     StructField("rkc__Current_Likelihood_Number__c", StringType(), True),
                     StructField("rkc__Current_Likelihood__c", StringType(), True),
                     StructField("rkc__Current_Risk_Score__c", StringType(), True),
                     StructField("rkc__Days_Since_Last_Modified__c", StringType(), True),
                     StructField("rkc__Hierarchy__c", StringType(), True),
                     StructField("rkc__Inherent_Impact_Decimal__c", StringType(), True),
                     StructField("rkc__Inherent_Impact_Number__c", StringType(), True),
                     StructField("rkc__Inherent_Impact__c", StringType(), True),
                     StructField("rkc__Inherent_Likelihood_Decimal__c", StringType(), True),
                     StructField("rkc__Inherent_Likelihood_Number__c", StringType(), True),
                     StructField("rkc__Inherent_Likelihood__c", StringType(), True),
                     StructField("rkc__Inherent_Risk_Score__c", StringType(), True),
                     StructField("rkc__Inherent_Velocity_Decimal__c", StringType(), True),
                     StructField("rkc__Inherent_Velocity_Number__c", StringType(), True),
                     StructField("rkc__Inherent_Velocity__c", StringType(), True),
                     StructField("rkc__Reduction_in_Risk_Score__c", StringType(), True),
                     StructField("rkc__Risk_Category__c", StringType(), True),
                     StructField("rkc__Risk_Description_Long__c", StringType(), True),
                     StructField("rkc__Risk_Description__c", StringType(), True),
                     StructField("rkc__Risk_Direction__c", StringType(), True),
                     StructField("rkc__Risk_Library__c", StringType(), True),
                     StructField("rkc__Risk_Owner_Contact__c", StringType(), True),
                     StructField("rkc__Risk_Owner_User__c", StringType(), True),
                     StructField("rkc__Risk_Status__c", StringType(), True),
                     StructField("rkc__Risk_Subcategory__c", StringType(), True),
                     StructField("rkc__Risk_Treatment__c", StringType(), True),
                     StructField("rkc__Risk_Volatility__c", StringType(), True),
                     StructField("rkc__Risk_or_Opportunity__c", StringType(), True),
                     StructField("rkc__Root_Cause__c", StringType(), True),
                     StructField("rkc__Target_Impact_Decimal__c", StringType(), True),
                     StructField("rkc__Target_Impact_Number__c", StringType(), True),
                     StructField("rkc__Target_Impact__c", StringType(), True),
                     StructField("rkc__Target_Likelihood_Decimal__c", StringType(), True),
                     StructField("rkc__Target_Likelihood_Number__c", StringType(), True),
                     StructField("rkc__Target_Likelihood__c", StringType(), True),
                     StructField("rkc__Target_Risk_Score__c", StringType(), True),
                     StructField("rkc__Most_Recent_Assessment_Date__c", StringType(), True),
                     StructField("rkc__Number_of_Controls__c", StringType(), True),
                     StructField("rkc__Number_of_Risk_Assessments__c", StringType(), True),
                     StructField("Basis_for_Assessment__c", StringType(), True),
                     StructField("Business_Line__c", StringType(), True),
                     StructField("Comments__c", StringType(), True),
                     StructField("Consequences__c", StringType(), True),
                     StructField("Current_Impact_Syt__c", StringType(), True),
                     StructField("Current_Likelihood_Syt__c", StringType(), True),
                     StructField("Current_RAG_Rating_Internal__c", StringType(), True),
                     StructField("Current_RAG_Rating__c", StringType(), True),
                     StructField("Date_Risk_Review_Sent__c", StringType(), True),
                     StructField("Email_Syngenta_User__c", StringType(), True),
                     StructField("Financial_Impact_m__c", StringType(), True),
                     StructField("Impact_Type__c", StringType(), True),
                     StructField("Node_Code__c", StringType(), True),
                     StructField("Open_the_Risk_for_Review__c", StringType(), True),
                     StructField("Previous_Risk_Ref__c", StringType(), True),
                     StructField("Review_End_Date__c", StringType(), True),
                     StructField("Review_Start_Date__c", StringType(), True),
                     StructField("Risk_Library_Name__c", StringType(), True),
                     StructField("Risk_Matrix_Definitions__c", StringType(), True),
                     StructField("Risk_Name_Original__c", StringType(), True),
                     StructField("Risk_Owner__c", StringType(), True),
                     StructField("Risk_Reference_No__c", StringType(), True),
                     StructField("Risk_Reference__c", StringType(), True),
                     StructField("Risk_Sub_Type_Definitions__c", StringType(), True),
                     StructField("Risk_Sub_Type__c", StringType(), True),
                     StructField("Risk_Type__c", StringType(), True),
                     StructField("Send_Risk_Review_Email__c", StringType(), True),
                     StructField("Single_Source__c", StringType(), True),
                     StructField("Strategic_objective__c", StringType(), True),
                     StructField("Supplier__c", StringType(), True),
                     StructField("Target_Impact_Syt__c", StringType(), True),
                     StructField("Target_Likelihood_Syt__c", StringType(), True),
                     StructField("Target_RAG_Rating_Internal__c", StringType(), True),
                     StructField("Target_RAG_Rating__c", StringType(), True),
                     StructField("Triggers__c", StringType(), True),
                     StructField("Update__c", StringType(), True),
                     StructField("Risk_Owner_Free_Text__c", StringType(), True),
                     StructField("Hierarchy_Free_Text__c", StringType(), True),
                     StructField("Supplier_Name_Original__c", StringType(), True),
                     StructField("Count_Open_Action_Plans__c", StringType(), True),
                     StructField("Missing_Control_Effectiveness__c", StringType(), True),
                     StructField("Missing_Open_Action_Plans_OR_Control__c", StringType(), True),
                     StructField("Missing_Risk_Description__c", StringType(), True),
                     StructField("Missing_Risk_Owner__c", StringType(), True),
                     StructField("Missing_Triggers_or_Consequences__c", StringType(), True),
                     StructField("No_of_Controls_with_No_Effectiveness__c", StringType(), True),
                     StructField("Division_from_hierarchy__c", StringType(), True),
                     StructField("Risk_18_Digit_ID__c", StringType(), True),
                     StructField("CP_Agro_Design_Code__c", StringType(), True),
                     StructField("Missing_risk_description_long__c", StringType(), True),
                     StructField("Insurance__c", StringType(), True),
                     StructField("Source_Type__c", StringType(), True),
                     StructField("PPCR__c", StringType(), True),
                     StructField("rkc__X10_K_Risk__c", StringType(), True),
                     StructField("rkc__Current_Velocity__c", StringType(), True),
                     StructField("rkc__Current_Velocity_Decimal__c", StringType(), True),
                     StructField("rkc__Current_Velocity_Number__c", StringType(), True),
                     StructField("Group_Risk__c", StringType(), True),
                     StructField("Location__c", StringType(), True)
                     })

spark = SparkSession.builder.getOrCreate()

# Create DF Reader with specific schema
dataframe_reader = spark.read.format("parquet").schema(schema)

# Call load() to actually read the data and create a DataFrame
dataframe = dataframe_reader.load(url)

# Create new DF where we replace None strings with null
dataframe2 = dataframe.replace("None", None)

# Create temp view called RISK
dataframe2.createOrReplaceTempView('RISK')

# Do some dummy processing
print(spark.sql("""

SELECT
    NULLIF(riskdata.id,'') as risk_id
    ,case when riskdata.isdeleted = 'False' then false
          when riskdata.isdeleted = 'True' then true else null end               as risk_is_deleted
   ,MD5(id) as md5_id       
   ,NULLIF(riskdata.rkc__X10_K_Risk__c, '')                                      as risk
   ,NULLIF(riskdata.rkc__Action_Plan__c, '')                                     as risk_action_plan
   ,NULLIF(riskdata.rkc__AM_Best_Major_Category__c, '')                          as am_best_major_category
   ,NULLIF(riskdata.rkc__AM_Best_Minor_Category__c, '')                          as am_best_minor_category
   ,NULLIF(riskdata.Basis_for_Assessment__c, '')                                 as basis_for_assessment
   ,NULLIF(riskdata.Business_Line__c, '')                                        as risk_business_line
   ,NULLIF(riskdata.rkc__Risk_Category__c, '')                                   as risk_category
   ,NULLIF(riskdata.Comments__c, '')                                             as risk_comments
   ,NULLIF(riskdata.Consequences__c, '')                                         as risk_consequences
    
    from RISK as riskdata

""").show())

