import pandas as pd
import pymongo

sit_connection_string = "mongodb://cogx_plat_api:CogpSiT%40ds2019@va33tlvmdb407.wellpoint.com:37043/?authSource=cognitive-platform-sit_system&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
sit_client = pymongo.MongoClient(sit_connection_string)
sit_clm2 = sit_client["cognitive-platform-sit_claimsol2"]
sit_csbd = sit_clm2["intelliauth_csdb_llm"]
sit_gbd = sit_clm2["intelliauth_gbd_llm"]

perf_connection_string = "mongodb://cogx_plat_api:CogpPerF%40ds2019@va10tlvmdb360.wellpoint.com:37043/?authSource=cognitive-platform-perf_system&replicaSet=cogDV04&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
perf_client = pymongo.MongoClient(perf_connection_string)
perf_clm2 = perf_client["cognitive-platform-perf_claimsol2"]
perf_csbd = perf_clm2["intelliauth_csdb_llm"]
perf_gbd = perf_clm2["intelliauth_gbd_llm"]

prod_connection_string = "mongodb://cogx_plat_bat:Z5AajjWQ3aCQAEUY@va10plvmdb412.wellpoint.com:37043/?authSource=cognitive-platform_system&replicaSet=cogPR02&readPreference=primary&appname=MongoDB%20Compass&ssl=true"
prod_client = pymongo.MongoClient(prod_connection_string)
prod_clm2 = prod_client["cognitive-platform_claimsol2"]
prod_csbd = prod_clm2["intelliauth_csdb_llm"]
prod_gbd = prod_clm2["intelliauth_gbd_llm"]

col = ["CaseId", "pre_process_status", "pre_process_status_summary", "Auth_Type", "LOS", "case_speciality",
       "case_type", "POS", "DCNs", "ocr_not_found_dcns", "Plan_State", "LOB", "Fun_type", "Product_type", "Exclude/Include"]
pre_process_df = pd.DataFrame(columns=col)

auths = ["UM46534755"]
exe_auths = []
# PreProcessing Testing
for i in auths:
    qry_rslt = list(sit_csbd.find({"auth_id": i}).sort("_id", -1))
    if len(qry_rslt) == 0:
        qry_rslt = list(sit_gbd.find({}))
    for resp in qry_rslt:
        # Fetch Required fields
        ath = resp.get("auth_id", "")
        if ath not in exe_auths:
            print(ath)
            ath_type = resp.get("pre_processing", {}).get("extracted_fields", {}).get("type_of_auth", "")
            los_type = resp.get("pre_processing", {}).get("extracted_fields", {}).get("lengthOfStays", {}).get("losType",
                                                                                                              "")
            case_speciality = resp.get("pre_processing", {}).get("extracted_fields", {}).get("case_speciality", "")
            lob = resp.get("pre_processing", {}).get("extracted_fields", {}).get("lob", "")
            case_type = resp.get("pre_processing", {}).get("extracted_fields", {}).get("case_type", "")
            pos = resp.get("pre_processing", {}).get("extracted_fields", {}).get("pos", "")
            dcns = resp.get("pre_processing", {}).get("extracted_fields", {}).get("dcns", "")
            ocr_obj = resp.get("pre_processing", {}).get("ocr_output", {})

            pre_process_status_summary = ""
            pre_process_status = True
            # Scope or excluded
            if ath_type == "INPATIENT":
                pre_process_status_summary = pre_process_status_summary + "is_IP_policy: True"
            else:
                pre_process_status = False
                pre_process_status_summary = pre_process_status_summary + "is_IP_policy: False"

            if los_type == "IR":
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_IR_req: True"
            else:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_IR_req: False"
                pre_process_status = False

            if lob in ["LOC", "NATL", "HLK", "PPO", "UNICARE", "FEP", "CHIP", "MRMIP", "NATIONAL", "LOCAL", "MEDICAID", "MEDICARE"]:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_valid_lob: True"
            else:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_valid_lob: False"
                pre_process_status = False

            if case_type in ["ME", "SU", "MT", "NB"]:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_valid_Case_type: True"
            else:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_valid_Case_type: False"
                pre_process_status = False

            if pos == "21":
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_POS_21: True"
            else:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_POS_21: False"
                pre_process_status = False

            if case_speciality != "PSCCR":
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_not_PSCCR_case: True"
            else:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_not_PSCCR_case: False"
                pre_process_status = False

            dcn_ocrs = [docr.get("dcn") for docr in ocr_obj if ocr_obj.get("ocr_extract", "") == "success"]
            ocr_found = [dcn for dcn in dcns if dcn in dcn_ocrs]
            ocr_not_found = [dcn for dcn in dcns if dcn not in dcn_ocrs]
            ocr_not_dcn = [dcn for dcn in dcn_ocrs if dcn not in dcns]

            if len(dcns) == len(ocr_found):
                if all([dcn in dcn_ocrs for dcn in dcns]):
                    pre_process_status_summary = pre_process_status_summary + "\n" + "is_all_dcn_processed: True"
                else:
                    pre_process_status_summary = pre_process_status_summary + "\n" + "is_all_dcn_processed: False"
                    pre_process_status = False
            else:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_all_dcn_processed: False"
                pre_process_status = False

            pln_st = resp.get("pre_processing", {}).get("extracted_fields", {}).get("plan_state", "")
            fn_type = resp.get("pre_processing", {}).get("extracted_fields", {}).get("fund_type", "")
            prod_type = resp.get("pre_processing", {}).get("extracted_fields", {}).get("product_type", "")

            exclusion_file = pd.read_excel(r"C:\IA\IA-LLM\Docs\Exclusion\Exclusions for PhaseI 07_21-2.xlsx", sheet_name="Phase I Exclusions")
            exclusion_file.fillna("", inplace=True)
            if lob == "FEP":
                lob = "FEDERAL EMPLOYEE PROGRAM"
            elif lob == "LOC":
                lob = "LOCAL COMMERCIAL"
            is_exclude = exclusion_file[(exclusion_file.MBR_PLN_ST_CD == pln_st) & (exclusion_file.MBR_LOB_NM == lob)
                                        & (exclusion_file.FUNDG_TYPE_CD == fn_type)
                                        & (exclusion_file.PROD_FMLY_CD == prod_type)]
            pshI = list(is_exclude["Phase I"])[0]
            if pshI == "EXCLUDED":
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_not_Excluded: False"
                pre_process_status = False
            else:
                pre_process_status_summary = pre_process_status_summary + "\n" + "is_not_Excluded: True"

            row = [ath, pre_process_status, pre_process_status_summary, ath_type, los_type, case_speciality, case_type,
                   pos, dcns, ocr_not_found, pln_st, lob, fn_type, prod_type, pshI]

            pre_process_df.loc[len(pre_process_df.index)] = row

            exe_auths.append(ath)


pre_process_df.to_excel(r"C:\IA\IA-LLM\pre_test.xlsx")

