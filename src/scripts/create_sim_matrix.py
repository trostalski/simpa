import os
import sys

sys.path.insert(0, os.path.abspath(".."))
print(sys.path)

from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm

from db import PostgresDB

load_dotenv()


# generation of sim value calculation
EXP_PATH = "./TR1"


def main():
    Path(EXP_PATH).mkdir(parents=True, exist_ok=True)

    db = PostgresDB(
        db_name=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    similarity_values = (
        db.get_all_similarity_values()
    )  # (hadm_id_a, hadm_id_b, demo, icd, lab, vitals, input)

    hadm_ids = list(set([t[0] for t in similarity_values]))
    base_df = pd.DataFrame(index=hadm_ids, columns=hadm_ids)

    demo_df = base_df.copy()
    icd_df = base_df.copy()
    lab_df = base_df.copy()
    vital_df = base_df.copy()
    input_df = base_df.copy()

    for i in tqdm(range(len(similarity_values))):
        input_tuple = similarity_values[i]
        hadm_id_a = input_tuple[0]
        hadm_id_b = input_tuple[1]
        demo_value = input_tuple[2]
        icd_value = input_tuple[3]
        lab_value = input_tuple[4]
        vital_value = input_tuple[5]
        input_value = input_tuple[6]

        demo_df.at[hadm_id_a, hadm_id_b] = demo_value
        demo_df.at[hadm_id_b, hadm_id_a] = demo_value

        icd_df.at[hadm_id_a, hadm_id_b] = icd_value
        icd_df.at[hadm_id_b, hadm_id_a] = icd_value

        lab_df.at[hadm_id_a, hadm_id_b] = lab_value
        lab_df.at[hadm_id_b, hadm_id_a] = lab_value

        vital_df.at[hadm_id_a, hadm_id_b] = vital_value
        vital_df.at[hadm_id_b, hadm_id_a] = vital_value

        input_df.at[hadm_id_a, hadm_id_b] = input_value
        input_df.at[hadm_id_b, hadm_id_a] = input_value

    demo_df.to_pickle(
        "./{EXP_NAME}", compression="infer", protocol=5, storage_options=None
    )
    icd_df.to_pickle(
        "./{EXP_NAME}", compression="infer", protocol=5, storage_options=None
    )
    lab_df.to_pickle(
        "./{EXP_NAME}", compression="infer", protocol=5, storage_options=None
    )
    vital_df.to_pickle(
        "./{EXP_NAME}", compression="infer", protocol=5, storage_options=None
    )
    input_df.to_pickle(
        "./{EXP_NAME}", compression="infer", protocol=5, storage_options=None
    )


if __name__ == "__main__":
    main()
