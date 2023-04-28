import os
import sys

sys.path.insert(0, os.path.abspath(".."))
print(sys.path)

from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import numpy as np

from simpa.src.db import PostgresDB

load_dotenv()


# generation of sim value calculation
EXP_PATH = "simpa/src/scripts/TR1"


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
    )  # (hadm_id_a, hadm_id_b, demo, icd, lab, vitals, input, lab_24h, vitals_24h, prescription)

    matrix_skel = {}
    hadm_ids = list(set([i[0] for i in similarity_values]))

    for sim in similarity_values:
        hadm_a = sim[0]
        hadm_b = sim[1]
        if hadm_a not in matrix_skel:
            matrix_skel[tuple(sorted([hadm_a, hadm_b]))] = sim[2:]

    demo_sims = np.zeros((len(hadm_ids), len(hadm_ids)))
    icd_sims = np.zeros((len(hadm_ids), len(hadm_ids)))
    lab_sims = np.zeros((len(hadm_ids), len(hadm_ids)))
    lab_first_24h_sims = np.zeros((len(hadm_ids), len(hadm_ids)))
    vital_sims = np.zeros((len(hadm_ids), len(hadm_ids)))
    vital_first24h_sims = np.zeros((len(hadm_ids), len(hadm_ids)))
    input_sims = np.zeros((len(hadm_ids), len(hadm_ids)))
    prescription_sims = np.zeros((len(hadm_ids), len(hadm_ids)))

    for i in range(len(hadm_ids)):
        for j in range(len(hadm_ids)):
            m_key = tuple(sorted([hadm_ids[i], hadm_ids[j]]))
            if i == j:
                demo_sims[i][j] = 1
                icd_sims[i][j] = 1
                lab_sims[i][j] = 1
                lab_first_24h_sims[i][j] = 1
                vital_sims[i][j] = 1
                vital_first24h_sims[i][j] = 1
                input_sims[i][j] = 1
                prescription_sims[i][j] = 1
            else:
                demo_sims[i][j] = matrix_skel[m_key][0] if matrix_skel[m_key][0] else 0
                icd_sims[i][j] = matrix_skel[m_key][1] if matrix_skel[m_key][1] else 0
                lab_sims[i][j] = matrix_skel[m_key][2] if matrix_skel[m_key][2] else 0
                vital_sims[i][j] = matrix_skel[m_key][3] if matrix_skel[m_key][3] else 0
                input_sims[i][j] = matrix_skel[m_key][4] if matrix_skel[m_key][4] else 0
                lab_first_24h_sims[i][j] = (
                    matrix_skel[m_key][5] if matrix_skel[m_key][2] else 0
                )
                vital_first24h_sims[i][j] = (
                    matrix_skel[m_key][6] if matrix_skel[m_key][3] else 0
                )
                prescription_sims[i][j] = (
                    matrix_skel[m_key][7] if matrix_skel[m_key][5] else 0
                )

    demo_df = pd.DataFrame(demo_sims, index=hadm_ids, columns=hadm_ids)
    icd_df = pd.DataFrame(icd_sims, index=hadm_ids, columns=hadm_ids)
    lab_df = pd.DataFrame(lab_sims, index=hadm_ids, columns=hadm_ids)
    lab_first24h_df = pd.DataFrame(lab_first_24h_sims, index=hadm_ids, columns=hadm_ids)
    vital_df = pd.DataFrame(vital_sims, index=hadm_ids, columns=hadm_ids)
    vital_first24h_df = pd.DataFrame(
        vital_first24h_sims, index=hadm_ids, columns=hadm_ids
    )
    input_df = pd.DataFrame(input_sims, index=hadm_ids, columns=hadm_ids)
    prescription_df = pd.DataFrame(prescription_sims, index=hadm_ids, columns=hadm_ids)

    demo_df.to_pickle(
        f"./{EXP_PATH}/demographics.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )
    icd_df.to_pickle(
        f"./{EXP_PATH}/icd.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )
    lab_df.to_pickle(
        f"./{EXP_PATH}/lab.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )
    lab_first24h_df.to_pickle(
        f"./{EXP_PATH}/lab_first_24h.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )
    vital_df.to_pickle(
        f"./{EXP_PATH}/vitals.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )
    vital_first24h_df.to_pickle(
        f"./{EXP_PATH}/vitals_first_24h.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )
    input_df.to_pickle(
        f"./{EXP_PATH}/input.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )
    prescription_df.to_pickle(
        f"./{EXP_PATH}/prescription.pickle",
        compression="infer",
        protocol=5,
        storage_options=None,
    )


if __name__ == "__main__":
    main()
