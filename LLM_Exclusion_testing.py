import pandas as pd
import pymongo
from tabulate import tabulate

sit_connection_string = "mongodb://cogx_plat_api:CogpSiT%40ds2019@va33tlvmdb407.wellpoint.com:37043/?authSource=cognitive-platform-sit_system&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
sit_client = pymongo.MongoClient(sit_connection_string)
sit_claim_sol2 = sit_client["cognitive-platform-sit_claimsol2"]
sit_com_op = sit_claim_sol2["intelliauth_commercial_op"]
sit_gbd = sit_claim_sol2["intelliauth_gbd"]
sit_db2 = sit_client["cognitive-platform-sit_claimsol7"]
sit_dna_coll = sit_db2["dna_auth"]
sit_intelliauth_new_exclusions = sit_claim_sol2["intelliauth_exclusions"]
sit_llm_exclusion = sit_claim_sol2["ia_llm_exclusions"]

perf_connection_string = "mongodb://cogx_plat_api:CogpPerF%40ds2019@va10tlvmdb360.wellpoint.com:37043/?authSource=cognitive-platform-perf_system&replicaSet=cogDV04&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
perf_client = pymongo.MongoClient(perf_connection_string)
perf_db = perf_client["cognitive-platform-perf_claimsol2"]
perf_coll1 = perf_db["intelliauth_commercial_op"]
perf_coll2 = perf_db["intelliauth_gbd"]
dna_perf_connection_string = "mongodb://cogx_plat_api:CogpPerF%40ds2019@va10tlvmdb360.wellpoint.com:37045/?authSource=cognitive-platform-perf_system&readPreference=nearest&appname=MongoDB%20Compass&ssl=true"
dna_perf_client = pymongo.MongoClient(dna_perf_connection_string)
perf_db2 = dna_perf_client["cognitive-platform-perf_claimsol2"]
perf_dna_coll = perf_db2["dna_auth"]
perf_rnd_config = perf_db["intelliauth_RnD_policy_config"]
perf_intelliauth_new_exclusions = perf_db["intelliauth_exclusions"]

df1 = pd.read_excel(r"C:\IA\IA-LLM\Docs\Exclusion\Exclusions for PhaseI 07_21-2.xlsx", sheet_name="Phase I Exclusions")
# , sheet_name="Phase I Exclusions"

df1.fillna("", inplace=True)

df1.loc[df1["MBR_LOB_NM"] == "FEDERAL EMPLOYEE PROGRAM", "MBR_LOB_NM"] = "FEP"
df1.loc[df1["MBR_LOB_NM"] == "LOCAL COMMERCIAL", "MBR_LOB_NM"] = "LOC"

cols = ["PlanStateSold", "LOB", "Fund_Type", "Product_Type", "PhaseI", "check"]
doc = pd.DataFrame(columns=cols)

for i in df1.iterrows():
    pln_st, lob, fun_type, prod_type, phase = [str(v).strip() for v in list(i)[1]]
    res = list(
        sit_llm_exclusion.find({"PlanStateSold": pln_st, "LOB": lob, "Fund_Type": fun_type, "Product_Type": prod_type},
                               {"Action": 1}))
    flg = False
    if len(res) > 0:
        if phase == res[0].get("Action"):
            flg = True
    else:
        flg = "none"
    row = [pln_st, lob, fun_type, prod_type, phase, flg]
    doc.loc[len(doc.index)] = row
    row = ["NaN" if col == "" else col for col in row]
    # print(tabulate([row]))

    # print(res)
    print("{} ---> {} ---> {} ---> {} ---> {} ---> {}".format(row[0], row[1], row[2], row[3], row[4], row[5]))

doc.to_excel(r"C:\IA\IA-LLM\Testing\Exclusion_Testing\Test.xlsx")
