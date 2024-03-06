import logging
import sys
import pyspark.sql.functions as F
from joblib.executor import ETLExecuter, run
from joblib.models.dwh_tables import OU
from joblib.util.utils import stg_factperiodic_common_filter

logger = logging.Logger(__file__)
logging.basicConfig(level=logging.INFO)


class JobExecuter(ETLExecuter):
    def __init__(self, job_name, input, output, params, glue_context):
        super().__init__(job_name, input, output, params, glue_context)
        
    def transform(self):
        for tbl in self.rdfs.keys():
            stg_factperiodic_common_filter(self.rdfs[tbl], self.data_date).createOrReplaceTempView(tbl)
        sql = f"""
            select
                sha2(concat_ws('', 'IP|TBAADM.SOL', a.sol_id), 256) as ou_id
                ,a.sol_id as ou_code
                ,a.br_code as prn_ou_id
                ,null as cnrl_bnk_code
                ,b.set_id as area_code
                ,c.set_desc as area_nm
                ,a.city_code as city_code
                ,a.CITY_CODE as city_nm
                ,sha2('CL|SRC_STMTBAADM.SOL', 256) as src_stm_id
                ,to_date('{self.data_date}', 'yyyy-MM-dd') as ppn_dt
            from sol a
            left join sst b on a.sol_id = b.sol_id and b.del_flg = 'N' 
                and upper(b.set_id) in ('NORTH', 'MIDDLE', 'SOUTEAST', 'HCMC', 'SOUTH')
            left join stid c on b.set_id = c.set_id
            left join rct d on a.city_code = d.ref_code and d.ref_rec_type = '01'
        """
        df_trans = self.glue_context.sql(sql)
        columns_null = [ c for c in OU.items() if c not in df_trans.columns]
        for c in columns_null :
            df_trans = df_trans.withColumn(
                c,
                F.lit(None)
            )

        df_output = df_trans.select(
            *OU.items()
        )

        return df_output

def main(argv):
    run(argv, JobExecuter, logger)

if __name__ == "__main__":
    main(sys.argv)
