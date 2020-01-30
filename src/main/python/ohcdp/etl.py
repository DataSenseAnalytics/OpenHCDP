#- global imports start -
import smv
import dsalib
import pyspark.sql.functions as F
import pyspark.sql.window as W
import pyspark.sql.types as T
#- global imports end -
#- global utils imports start -
from dsalib.utils import pandasToSpark
#- global utils imports end -
#- fullNpiList imports start -
import ohcdp.input
#- fullNpiList imports end -
class fullNpiList(dsalib.DsaPySparkModule):
    """
    Relevant columns of the full NPI list
    """
    # Module UUID: dd1e3352-d2e1-43d7-8ee2-85fcaefebb9d
    #- fullNpiList refs start -
    def ohcdp_input_npiMaster(self): return ohcdp.input.npiMaster
    #- fullNpiList refs end -
    def run(self, i):
        #- pre-run start -
        npiMaster = i[self.ohcdp_input_npiMaster()]
        #- pre-run end -
        # Default Libraries:
        #	pandas as pd
        #	pyspark.sql.functions as F
        # Input module name(s) (e.g. npiMaster) can be used directly as the DataFrame which it outputs.
        return npiMaster.select(
            F.col('NPI').alias('id'),
            F.col('Provider_First_Name').alias('First_Name'),
            F.col('Provider_Last_Name_Legal_Name').alias('Last_Name'),
            F.col('Provider_First_Line_Business_Practice_Location_Address').alias('Line_1_Street_Address'),
            F.col('Provider_Business_Practice_Location_Address_Postal_Code').alias('Zip_Code'),
            F.col('Healthcare_Provider_Taxonomy_Code_1')
        ).filter(F.length(F.col('Zip_Code')) >= 5)
    def requiresConfig(self):
        return []
    def requiresLib(self):
        return []
    def requiresDS(self):
        return [self.ohcdp_input_npiMaster()]
    def isEphemeral(self):
        return False