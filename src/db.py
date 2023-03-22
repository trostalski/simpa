import psycopg2

from schemas import Demographics, ICDDiagnosis, LabEvent, Vitalsign, InputEvent
import sql_queries as sq
from constants import VITAL_SIGN_NAMES, VITALSIGN_STATISTICS


class PostgresDB:
    def __init__(self, db_name, host="localhost", port=5432, user=None, password=None):
        self.db_name = db_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            print("Connected to database")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        if self.conn:
            self.conn.close()

    def execute_query(self, query, parameters=None):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, parameters)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.conn.rollback()
            print(f"Error executing query: {e}")

    def get_random_hadm_ids(self, n: int):
        """Get n random hadm_ids."""
        query = sq.random_hadm_ids
        return [hadm_id for hadm_id, in self.execute_query(query, (n,))]

    # Get categories
    def get_patient_demographics(self, hadm_ids: list[int]):
        """Get age, gender and race for a list of subject_ids."""
        result = []
        query = sq.get_demographics
        db_result = self.execute_query(query, (hadm_ids,))
        for subject_id, hadm_id, age, gender, ethnicity in db_result:
            result.append(
                Demographics(
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    age=age,
                    gender=gender,
                    ethnicity=ethnicity,
                )
            )
        return result

    def get_mean_vitalsigns(self, hadm_ids: list[int]):
        """Get mean vital signs for a list of hadm_ids."""
        result = []
        vital_sign_names = VITAL_SIGN_NAMES
        query = sq.get_mean_vitalsigns
        db_result = self.execute_query(query, (hadm_ids,))
        for values in db_result:
            subject_id = values[0]
            hadm_id = values[1]
            for name, value in zip(vital_sign_names, values[2:]):
                mean = VITALSIGN_STATISTICS[name]["mean"]
                std_dev = VITALSIGN_STATISTICS[name]["std"]
                result.append(
                    Vitalsign(
                        id=name,
                        subject_id=subject_id,
                        hadm_id=hadm_id,
                        name=name,
                        value=value,
                        id_mean=mean,
                        id_std_dev=std_dev,
                    )
                )
        return result

    def get_mean_labevents(self, hadm_ids: list[int]):
        result = []
        query = sq.get_mean_labevents
        db_result = self.execute_query(query, (hadm_ids,))
        for (
            itemid,
            subject_id,
            hadm_id,
            valuenum,
            valueuom,
            mean_value,
            std_dev,
        ) in db_result:
            result.append(
                LabEvent(
                    id=itemid,
                    itemid=itemid,
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    value=valuenum,
                    valueuom=valueuom,
                    id_mean=mean_value,
                    id_std_dev=std_dev,
                )
            )
        return result

    def get_icd_diagnoses(self, hadm_ids: list[int]):
        result = []
        query = sq.get_icd_diagnoses
        db_result = self.execute_query(query, (hadm_ids,))
        for subject_id, hadm_id, seq_num, icd_code, icd_version in db_result:
            icd_code = icd_code.strip()  # icd_codes have trailing whitespace
            result.append(
                ICDDiagnosis(
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    seq_num=seq_num,
                    icd_code=icd_code,
                    icd_version=icd_version,
                )
            )
        return result

    def get_inputevents(self, hadm_ids: list[int]):
        result = []
        query = sq.get_inputevents
        db_result = self.execute_query(query, (hadm_ids,))
        for (
            subject_id,
            hadm_id,
            itemid,
            amount,
            amountuom,
            ordercategoryname,
        ) in db_result:
            result.append(
                InputEvent(
                    value=itemid,
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    itemid=itemid,
                    amount=amount,
                    amountuom=amountuom,
                    ordercategoryname=ordercategoryname,
                )
            )
        return result

    # Get from similarity tables
    def get_all_similarity_values(self):
        query = sq.all_similarity_values
        db_result = self.execute_query(query)
        return db_result

    # Get statistics
    def get_labevent_mean_std_for_itemid(self, itemid: int):
        query = sq.labevent_mean_std
        db_result = self.execute_query(query, (itemid,))
        mean, std = db_result[0][0], db_result[0][1]
        return mean, std

    def get_vitalsign_mean_std_for_name(self, vitalsign_name: str):
        query = sq.vitalsign_mean_std
        db_result = self.execute_query(query, (vitalsign_name,))
        mean, std = db_result[0][0], db_result[0][1]
        return mean, std

    # Get endpoints
    def get_endpoints_for_hadm_id(self, hadm_id: int) -> tuple[int, int]:
        los_icu_result, los_hospital_result = None, None
        query = sq.endpoints_for_hadm_id
        db_result = self.execute_query(  # [(los_icu, los_hospital), ...]
            query,
            (hadm_id,),
        )

        for los_icu, los_hospital in db_result:
            if los_hospital is not None and los_hospital_result is None:
                los_hospital_result = round(float(los_hospital), 1)
            if los_icu is not None:
                if los_icu_result is None:
                    los_icu_result = round(float(los_icu), 1)
                else:
                    los_icu_result += round(float(los_icu), 1)

        return los_icu_result, los_hospital_result
