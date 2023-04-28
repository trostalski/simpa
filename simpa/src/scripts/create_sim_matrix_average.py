import os
import sys

sys.path.insert(0, os.path.abspath(".."))
print(sys.path)

from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Path to pickle files
DIR_PATH = "simpa/src/scripts/TR1"

# generation of sim value calculation
GENERATION = 1

# SIMILARITY SCORE
DEMO_WEIGHT = 0.10
ICD_WEIGHT = 0.10
LAB_WEIGHT = 0.00
LAB_F24H_WEIGHT = 0.40
VITALS_WEIGHT = 0.00
VITALS_F24H_WEIGHT = 0.20
INPUT_WEIGHT = 0.10
PRESCRIPTION_WEIGHT = 0.10
AGGREGATE = "mean"


def main():
    demo_df = pd.read_pickle(os.path.join(DIR_PATH, f"demographics.pickle"))
    icd_df = pd.read_pickle(os.path.join(DIR_PATH, f"icd.pickle"))
    lab_df = pd.read_pickle(os.path.join(DIR_PATH, f"lab.pickle"))
    lab_first_24h_df = pd.read_pickle(os.path.join(DIR_PATH, f"lab_first_24h.pickle"))
    vital_df = pd.read_pickle(os.path.join(DIR_PATH, f"vitals.pickle"))
    vital_first_24h_df = pd.read_pickle(
        os.path.join(DIR_PATH, f"vitals_first_24h.pickle")
    )
    input_df = pd.read_pickle(os.path.join(DIR_PATH, f"input.pickle"))
    prescription_df = pd.read_pickle(os.path.join(DIR_PATH, f"prescription.pickle"))

    result_df = (
        demo_df * DEMO_WEIGHT
        + icd_df * ICD_WEIGHT
        + lab_df * LAB_WEIGHT
        + lab_first_24h_df * LAB_F24H_WEIGHT
        + vital_df * VITALS_WEIGHT
        + vital_first_24h_df * VITALS_F24H_WEIGHT
        + input_df * INPUT_WEIGHT
        + prescription_df * PRESCRIPTION_WEIGHT
    )

    result_df.to_pickle(os.path.join(DIR_PATH, f"sim_matrix.pickle"))


if __name__ == "__main__":
    main()
