import os
import sys

print(sys.path)
from dotenv import load_dotenv
import logging
import asyncio
import asyncpg
from asyncpg import Connection
import time
from multiprocessing import Pool
import json
from datetime import datetime

import simpa.src.sql_queries as sq
from simpa.src.helper import batch, psycop_to_asyncpg_string, scale_to_range
from simpa.src.schemas import (
    Demographics,
    LabEvent,
    ICDDiagnosis,
    InputEvent,
    Vitalsign,
    Prescription,
)
from simpa.src.constants import (
    VITAL_SIGN_NAMES,
    VITALSIGN_STATISTICS,
    VITALSIGN_NORM_RANGE,
)
from simpa.src.labevents import LabEventComparator
from simpa.src.demographics import DemographicsComparator
from simpa.src.icd_diagnoses import ICDComparator
from simpa.src.inputevents import InputEventComparator
from simpa.src.vitalsigns import VitalsignComparator
from simpa.src.icd_diagnoses import load_icd10_graph
from simpa.src.prescriptions import PrescriptionComparator

########## PARAMETERS ##########
MIN_AGE = 18
MAX_AGE = 65
LIMIT = 400
BATCH_SIZE = 400
# GENDER = None

ICD_MAP_PATH = "simpa/src/sql/icd9_to_icd10_map.json"
################################

load_dotenv()
logger = logging.getLogger(__name__)

labevent_comp = LabEventComparator()
demographic_comp = DemographicsComparator()
icd_comp = ICDComparator()
inputevent_comp = InputEventComparator()
vitalsign_comp = VitalsignComparator()
prescriptions_comp = PrescriptionComparator()


async def insert_similarities(conn: Connection, similarites, table_name: str):
    """Insert similarities into database."""
    insert_list = [
        (
            s["encounter_a"],
            s["encounter_b"],
            s["similarity"]["demographics_sim"],
            s["similarity"]["diagnoses_sim"],
            s["similarity"]["labevents_sim"],
            s["similarity"]["labevents_first24h_sim"],
            s["similarity"]["vitalsigns_sim"],
            s["similarity"]["vitalsigns_first24h_sim"],
            s["similarity"]["inputevents_sim"],
            s["similarity"]["prescriptions_sim"],
        )
        for s in similarites
    ]
    await conn.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                hadm_id_a integer,
                hadm_id_b integer,
                demographics_similarity float,
                diagnoses_similarity float,
                labevents_similarity float,
                labevents_first24h_similarity float,
                vitalsigns_similarity float,
                vitalsigns_first24h_similarity float,
                inputevents_similarity float,
                prescriptions_similarity float,
                PRIMARY KEY (hadm_id_a, hadm_id_b)
            );

            CREATE INDEX IF NOT EXISTS similarity_hadm_id_a ON {table_name} (hadm_id_a);

            CREATE INDEX IF NOT EXISTS similarity_hadm_id_b ON {table_name} (hadm_id_b);
        """
    )
    await conn.executemany(
        f"""
            INSERT INTO 
                {table_name} (hadm_id_a, hadm_id_b, demographics_similarity, diagnoses_similarity, labevents_similarity,
                labevents_first24h_similarity, vitalsigns_similarity, vitalsigns_first24h_similarity, inputevents_similarity, prescriptions_similarity)
            VALUES 
            ( $1 , $2 , $3 , $4 , $5 , $6 , $7, $8, $9, $10 );
        """,
        insert_list,
    )


def normalize_categories(result: list[dict]) -> list[dict]:
    demographics_sims = [
        r["similarity"]["demographics_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    demographics_sims = [0 if d is None else d for d in demographics_sims]

    diagnoses_sims = [
        r["similarity"]["diagnoses_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    diagnoses_sims = [0 if d is None else d for d in diagnoses_sims]

    labevents_sims = [
        r["similarity"]["labevents_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    labevents_sims = [0 if d is None else d for d in labevents_sims]

    labevents_first24h_sims = [
        r["similarity"]["labevents_first24h_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    labevents_sims = [0 if d is None else d for d in labevents_first24h_sims]

    vitalsigns_sims = [
        r["similarity"]["vitalsigns_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    vitalsigns_sims = [0 if d is None else d for d in vitalsigns_sims]

    vitalsigns_first24h_sims = [
        r["similarity"]["vitlasigns_first24h_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    vitalsigns_sims = [0 if d is None else d for d in vitalsigns_first24h_sims]

    inputevents_sims = [
        r["similarity"]["inputevents_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    inputevents_sims = [0 if d is None else d for d in inputevents_sims]

    prescriptions_sims = [
        r["similarity"]["prescriptions_sim"]
        for r in result
        if r["encounter_a"] != r["encounter_b"]
    ]
    prescriptions_sims = [0 if d is None else d for d in prescriptions_sims]

    normalized_demographics_sims = scale_to_range(
        input_list=demographics_sims, range_start=0, range_end=1
    )
    normalized_diagnoses_sims = scale_to_range(
        input_list=diagnoses_sims, range_start=0, range_end=1
    )
    normalized_labevents_sims = scale_to_range(
        input_list=labevents_sims, range_start=0, range_end=1
    )
    normalized_labevents_first24h_sims = scale_to_range(
        input_list=labevents_first24h_sims, range_start=0, range_end=1
    )
    normalized_vitalsigns_sims = scale_to_range(
        input_list=vitalsigns_sims, range_start=0, range_end=1
    )
    normalized_vitalsigns_first24h_sims = scale_to_range(
        input_list=vitalsigns_first24h_sims, range_start=0, range_end=1
    )
    normalized_inputevents_sims = scale_to_range(
        input_list=inputevents_sims, range_start=0, range_end=1
    )
    normalized_prescriptions_sims = scale_to_range(
        input_list=prescriptions_sims, range_start=0, range_end=1
    )

    for i, r in enumerate([r for r in result if r["encounter_a"] != r["encounter_b"]]):
        r["similarity"]["demographics_sim"] = normalized_demographics_sims[i]
        r["similarity"]["diagnoses_sim"] = normalized_diagnoses_sims[i]
        r["similarity"]["labevents_sim"] = normalized_labevents_sims[i]
        r["similarity"]["labevents_first24h_sim"] = normalized_labevents_first24h_sims[
            i
        ]
        r["similarity"]["vitalsigns_sim"] = normalized_vitalsigns_sims[i]
        r["similarity"][
            "vitalsigns_first24h_sim"
        ] = normalized_vitalsigns_first24h_sims[i]
        r["similarity"]["inputevents_sim"] = normalized_inputevents_sims[i]
        r["similarity"]["prescriptions_sim"] = normalized_prescriptions_sims[i]
    return result


def clean_diagnoses_records(diagnoses):
    logger.info("Cleaning diagnoses.")
    result = []
    G = load_icd10_graph()
    good_c = 0
    bad_c = 0
    with open(ICD_MAP_PATH) as f:
        map = json.load(f)
        map = map["data"]
        for d_record in diagnoses:
            d = dict(d_record)
            d["icd_code"] = d["icd_code"].strip()
            icd_version = d["icd_version"]
            icd_code = d["icd_code"]
            if icd_version == 9:
                if icd_code in [m["code"] for m in map]:  # code in map
                    new_code = [m["icd10_code"] for m in map if m["code"] == icd_code]
                    d["icd_code"] = new_code[0]
                    d["icd_version"] = 10
                else:  # code not in map
                    d["icd_code"] = None
                    d["icd_version"] = None
            if d["icd_code"] in G.graph.nodes:
                good_c += 1
                result.append(d)
            else:
                bad_c += 1
    logger.info(
        f"Cleaned {bad_c} bad diagnoses records, {good_c} good diagnoses records"
    )
    return result


def compare_encounters(encounter_pair: tuple[dict, dict]):
    encounter_a = encounter_pair[0]
    encounter_b = encounter_pair[1]
    lab_sim = None
    lab_first24h_sim = None
    demo_sim = None
    icd_sim = None
    vitalsign_sim = None
    vitalsign_first24h_sim = None
    inputevent_sim = None

    if "labevents" in encounter_a and "labevents" in encounter_b:
        lab_sim = labevent_comp.compare(
            encounter_a["labevents"],
            encounter_b["labevents"],
            scale_by_distribution=True,
        )
    if "labevents_first24h" in encounter_a and "labevents_first24h" in encounter_b:
        lab_first24h_sim = labevent_comp.compare(
            encounter_a["labevents_first24h"],
            encounter_b["labevents_first24h"],
            scale_by_distribution=True,
        )
    if "demographics" in encounter_a and "demographics" in encounter_b:
        demo_sim = demographic_comp.compare(
            encounter_a["demographics"], encounter_b["demographics"]
        )
    if "diagnoses" in encounter_a and "diagnoses" in encounter_b:
        icd_sim = icd_comp.compare(encounter_a["diagnoses"], encounter_b["diagnoses"])
    if "inputevents" in encounter_a and "inputevents" in encounter_b:
        inputevent_sim = inputevent_comp.compare(
            encounter_a["inputevents"], encounter_b["inputevents"]
        )
    if "vitalsigns" in encounter_a and "vitalsigns" in encounter_b:
        vitalsign_sim = vitalsign_comp.compare(
            encounter_a["vitalsigns"], encounter_b["vitalsigns"]
        )
    if "vitalsigns_first24h" in encounter_a and "vitalsigns_first24h" in encounter_b:
        vitalsign_first24h_sim = vitalsign_comp.compare(
            encounter_a["vitalsigns_first24h"], encounter_b["vitalsigns_first24h"]
        )
    if "prescriptions" in encounter_a and "prescriptions" in encounter_b:
        prescriptions_sim = prescriptions_comp.compare(
            encounter_a["prescriptions"], encounter_b["prescriptions"]
        )

    sims = {
        "labevents_sim": lab_sim,
        "labevents_first24h_sim": lab_first24h_sim,
        "demographics_sim": demo_sim,
        "diagnoses_sim": icd_sim,
        "inputevents_sim": inputevent_sim,
        "vitalsigns_sim": vitalsign_sim,
        "vitalsigns_first24h_sim": vitalsign_first24h_sim,
        "prescriptions_sim": prescriptions_sim,
    }

    return {
        "encounter_a": encounter_a["hadm_id"],
        "encounter_b": encounter_b["hadm_id"],
        "similarity": sims,
    }


async def main(table_name: str):
    logger.info(f"Started similarity import for table {table_name}.")
    conn = await asyncpg.connect(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    logger.info("Connected to databases, starting to download categories.")

    hadm_ids = await conn.fetch(
        psycop_to_asyncpg_string(sq.sepsis_cohort), MIN_AGE, MAX_AGE, LIMIT
    )

    logger.info("Finished downloading hadm_ids for cohort from db.")
    demographics_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_demographics), hadm_ids
    )
    logger.info("Finished downloading demographics for cohort from db.")
    diagnoses_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_icd_diagnoses), hadm_ids
    )
    logger.info("Finished downloading diagnoses for cohort from db.")
    labevents_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_mean_labevents), hadm_ids
    )
    logger.info("Finished downloading labevents for cohort from db.")
    labevents_first24h_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_mean_labevents_first_24h_icu), hadm_ids
    )
    logger.info("Finished downloading labevents first 24h for cohort from db.")
    vitalsigns_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_mean_vitalsigns), hadm_ids
    )
    logger.info("Finished downloading vitalsigns for cohort from db.")
    vitalsigns_first24h_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_mean_vitalsigns_first_24h_icu), hadm_ids
    )
    logger.info("Finished downloading vitalsigns first 24h for cohort from db.")
    inputevents_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_inputevents), hadm_ids
    )
    logger.info("Finished downloading inputevents for cohort from db.")
    prescription_records = await conn.fetch(
        psycop_to_asyncpg_string(sq.get_prescriptions_icu), hadm_ids
    )
    logger.info("Finished downloading prescriptions for cohort from db.")

    logger.info("Starting to build similarity encounter for hadm_ids.")
    hadm_ids = [i["hadm_id"] for i in hadm_ids]
    hadm_data = {i: {"hadm_id": i} for i in hadm_ids}

    for i in demographics_records:
        hadm_data[i["hadm_id"]]["demographics"] = Demographics(
            subject_id=i["subject_id"],
            hadm_id=i["hadm_id"],
            age=i["age"],
            gender=i["gender"],
            ethnicity=i["race"],
        )

    diagnoses_dicts = clean_diagnoses_records(
        diagnoses_records
    )  # turns the records into dicts and removes bad records
    for i in diagnoses_dicts:
        if "diagnoses" not in hadm_data[i["hadm_id"]]:
            hadm_data[i["hadm_id"]]["diagnoses"] = []
        hadm_data[i["hadm_id"]]["diagnoses"].append(
            ICDDiagnosis(
                subject_id=i["subject_id"],
                hadm_id=i["hadm_id"],
                icd_version=i["icd_version"],
                icd_code=i["icd_code"],
                seq_num=i["seq_num"],
            )
        )

    for i in labevents_records:
        valuenum = i["valuenum"]
        if valuenum is None:
            continue
        if "labevents" not in hadm_data[i["hadm_id"]]:
            hadm_data[i["hadm_id"]]["labevents"] = []
        mean = i["mean_value"]
        std_dev = i["std_dev"]
        lower_ref = i["ref_range_lower"]
        upper_ref = i["ref_range_upper"]
        if upper_ref and lower_ref:
            is_abnormal = valuenum < lower_ref or valuenum > upper_ref
        elif mean and std_dev:
            is_abnormal = valuenum < mean - 2 * std_dev or valuenum > mean + 2 * std_dev
        else:
            is_abnormal = False
        hadm_data[i["hadm_id"]]["labevents"].append(
            LabEvent(
                id=i["itemid"],
                subject_id=i["subject_id"],
                itemid=i["itemid"],
                value=valuenum,
                valueuom=i["valueuom"],
                hadm_id=i["hadm_id"],
                id_mean=mean,
                id_std_dev=std_dev,
                abnormal=is_abnormal,
            )
        )

    for i in labevents_first24h_records:
        valuenum = i["valuenum"]
        if valuenum is None:
            continue
        if "labevents_first24h" not in hadm_data[i["hadm_id"]]:
            hadm_data[i["hadm_id"]]["labevents_first24h"] = []
        mean = i["mean_value"]
        std_dev = i["std_dev"]
        lower_ref = i["ref_range_lower"]
        upper_ref = i["ref_range_upper"]
        if upper_ref and lower_ref:
            is_abnormal = valuenum < lower_ref or valuenum > upper_ref
        elif mean and std_dev:
            is_abnormal = valuenum < mean - 2 * std_dev or valuenum > mean + 2 * std_dev
        else:
            is_abnormal = False
        hadm_data[i["hadm_id"]]["labevents_first24h"].append(
            LabEvent(
                id=i["itemid"],
                subject_id=i["subject_id"],
                itemid=i["itemid"],
                value=i["valuenum"],
                valueuom=i["valueuom"],
                hadm_id=i["hadm_id"],
                id_mean=i["mean_value"],
                id_std_dev=i["std_dev"],
                abnormal=is_abnormal,
            )
        )

    for i in vitalsigns_records:
        if "vitalsigns" not in hadm_data[i["hadm_id"]]:
            hadm_data[i["hadm_id"]]["vitalsigns"] = []
        subject_id = i[0]
        hadm_id = i[1]
        for name, value in zip(VITAL_SIGN_NAMES, i[2:]):
            if value is None:
                continue
            id_mean = VITALSIGN_STATISTICS[name]["mean"]
            id_std_dev = VITALSIGN_STATISTICS[name]["std"]
            is_abnormal = (
                value < VITALSIGN_NORM_RANGE[name]["lower"]
                or value > VITALSIGN_NORM_RANGE[name]["upper"]
            )
            hadm_data[i["hadm_id"]]["vitalsigns"].append(
                Vitalsign(
                    id=name,
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    name=name,
                    value=value,
                    id_mean=id_mean,
                    id_std_dev=id_std_dev,
                    abnormal=is_abnormal,
                )
            )
    for i in vitalsigns_first24h_records:
        if "vitalsigns_first24h" not in hadm_data[i["hadm_id"]]:
            hadm_data[i["hadm_id"]]["vitalsigns_first24h"] = []
        subject_id = i[0]
        hadm_id = i[1]
        for name, value in zip(VITAL_SIGN_NAMES, i[2:]):
            if value is None:
                continue
            id_mean = VITALSIGN_STATISTICS[name]["mean"]
            id_std_dev = VITALSIGN_STATISTICS[name]["std"]
            is_abnormal = (
                value < VITALSIGN_NORM_RANGE[name]["lower"]
                or value > VITALSIGN_NORM_RANGE[name]["upper"]
            )
            hadm_data[i["hadm_id"]]["vitalsigns_first24h"].append(
                Vitalsign(
                    id=name,
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    name=name,
                    value=value,
                    id_mean=id_mean,
                    id_std_dev=id_std_dev,
                    abnormal=is_abnormal,
                )
            )

    for i in inputevents_records:
        if "inputevents" not in hadm_data[i["hadm_id"]]:
            hadm_data[i["hadm_id"]]["inputevents"] = []
        hadm_data[i["hadm_id"]]["inputevents"].append(
            InputEvent(
                subject_id=i["subject_id"],
                hadm_id=i["hadm_id"],
                value=i["itemid"],
                itemid=i["itemid"],
                amount=i["amount"],
                amountuom=i["amountuom"],
                ordercategoryname=i["ordercategoryname"],
            )
        )

    for i in prescription_records:
        if "prescriptions" not in hadm_data[i["hadm_id"]]:
            hadm_data[i["hadm_id"]]["prescriptions"] = []
        hadm_data[i["hadm_id"]]["prescriptions"].append(
            Prescription(
                subject_id=i["subject_id"],
                hadm_id=i["hadm_id"],
                drug=i["drug"],
                pharmacy_id=i["pharmacy_id"],
                gsn=i["gsn"],
                value=i["value"],
            )
        )

    logger.info("Finished building similarity encounters.")

    encounter_pairs = []
    n = len(hadm_ids)
    batch_size = BATCH_SIZE
    for idx, data_batch in enumerate(batch(list(range(n)), batch_size)):
        for i in range(len(data_batch)):
            i = i + idx * batch_size
            hadm_id_i = hadm_ids[i]
            for j in range(i + 1, n):
                hadm_id_j = hadm_ids[j]
                pair = (
                    (hadm_data[hadm_id_i], hadm_data[hadm_id_j])
                    if hadm_id_i < hadm_id_j
                    else (hadm_data[hadm_id_j], hadm_data[hadm_id_i])
                )
                encounter_pairs.append(pair)

        with Pool(6) as pool:
            logger.info(f"Batch of {len(encounter_pairs)} created.")
            logger.info("Starting multiprocesses to calculate similarities")
            result = pool.map(compare_encounters, encounter_pairs)
        logger.info(f"Inserting a batch of {len(result)} into database.")

        await insert_similarities(conn=conn, similarites=result, table_name=table_name)
        encounter_pairs = []

    logger.info(f"Finished insertion, closing database conncetion.")
    await conn.close()


if __name__ == "__main__":
    table_name = "similarities_" + datetime.now().strftime("%Y%m%d%H%M%S")
    logging.basicConfig(
        filename=f"logs/{table_name}.log",
        filemode="a",
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )

    logger.info(
        f"PARAMETERS: MIN_AGE - {MIN_AGE}, MAX_AGE - {MAX_AGE}, LIMIT - {LIMIT}"
    )

    start_time = time.time()
    asyncio.run(main(table_name))
    async_time = time.time()
    logger.info(f"Total time: {async_time - start_time}")
