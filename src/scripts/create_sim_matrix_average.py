import os
import sys

sys.path.insert(0, os.path.abspath(".."))
print(sys.path)

from dotenv import load_dotenv
import pandas as pd
import numpy as np
from db import PostgresDB
from tqdm import tqdm

import dask.dataframe as dd
import dask.array as da
import dask.bag as db

load_dotenv()


# generation of sim value calculation
GENERATION = 1

# SIMILARITY SCORE
DEMO_WEIGHT = 0.00
ICD_WEIGHT = 0.00
LAB_WEIGHT = 1.00
VITALS_WEIGHT = 0.00
INPUT_WEIGHT = 0.00
AGGREGATE = "mean"

# CLUSTERING
N_CLUSTERS = 4


def create_similarity_matrix(
    similarity_list,
    demo_weight=DEMO_WEIGHT,
    icd_weight=ICD_WEIGHT,
    lab_weight=LAB_WEIGHT,
    vitals_weight=VITALS_WEIGHT,
    input_weight=INPUT_WEIGHT,
    aggregate="mean",
):
    print(f"Size of similarity list: {len(similarity_list)}")
    # Get a list of all patient IDs
    patient_ids = list(set([t[0] for t in similarity_list]))
    print("Finished retrieving patient ids")
    print(f"Size of patient ids: {len(patient_ids)}")

    # Create an empty matrix with NaN values
    similarity_matrix = pd.DataFrame(index=patient_ids, columns=patient_ids)
    print("Created matrix skeleton")

    # calculate the similarity value
    # Fill in the similarity values
    none_count = 0
    for i in tqdm(range(len(similarity_list))):
        input_tuple = similarity_list[i]
        demo_value = input_tuple[2]
        icd_value = input_tuple[3]
        lab_value = input_tuple[4]
        vitals_value = input_tuple[5]
        input_value = input_tuple[6]
        if (
            (input_value is None and input_weight > 0.00)
            or (demo_value is None and demo_weight > 0.00)
            or (icd_value is None and icd_weight > 0.00)
            or (lab_value is None and lab_weight > 0.00)
            or (vitals_value is None and vitals_weight > 0.00)
        ):
            none_count += 1
            sim_value = None
        elif aggregate == "mean":
            sim_value = (
                demo_weight * demo_value
                + icd_weight * icd_value
                + lab_weight * lab_value
                + vitals_weight * vitals_value
                + input_weight * input_value
            )
        similarity_matrix.at[similarity_list[i][0], similarity_list[i][1]] = sim_value
        similarity_matrix.at[similarity_list[i][1], similarity_list[i][0]] = sim_value
    print(f"Counted {none_count} numbers of none-similarities.")
    similarity_matrix = similarity_matrix.fillna(float(1.0))

    return similarity_matrix


def main():
    weights = [DEMO_WEIGHT, ICD_WEIGHT, LAB_WEIGHT, VITALS_WEIGHT, INPUT_WEIGHT]
    wc = [str(w).replace(".", "") for w in weights]
    file_name = f"sim{GENERATION}_D{wc[0]}IC{wc[1]}L{wc[2]}V{wc[3]}IP{wc[4]}"
    db = PostgresDB(
        db_name=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    similarity_values = db.get_all_similarity_values()
    df = create_similarity_matrix(
        similarity_values,
        demo_weight=DEMO_WEIGHT,
        icd_weight=ICD_WEIGHT,
        lab_weight=LAB_WEIGHT,
        vitals_weight=VITALS_WEIGHT,
        input_weight=INPUT_WEIGHT,
        aggregate=AGGREGATE,
    )
    df.to_pickle(
        f"./{file_name}.pickle", compression="infer", protocol=5, storage_options=None
    )


if __name__ == "__main__":
    main()
