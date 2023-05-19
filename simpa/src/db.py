import psycopg2
from simpa.src.helper import labevent_is_abnormal, vitalsign_is_abnormal

from simpa.src.schemas import (
    Demographics,
    ICDDiagnosis,
    LabEvent,
    Prescription,
    Vitalsign,
    InputEvent,
)
import simpa.src.sql_queries as sq
from simpa.src.constants import VITAL_SIGN_NAMES, VITALSIGN_STATISTICS


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
        query = sq.get_mean_vitalsigns_first_24h_icu
        db_result = self.execute_query(query, (hadm_ids,))
        for values in db_result:
            subject_id = values[0]
            hadm_id = values[1]
            for name, value in zip(vital_sign_names, values[2:]):
                mean = VITALSIGN_STATISTICS[name]["mean"]
                std_dev = VITALSIGN_STATISTICS[name]["std"]
                is_abnormal = vitalsign_is_abnormal(name=name, value=value)
                result.append(
                    Vitalsign(
                        id=name,
                        subject_id=subject_id,
                        hadm_id=hadm_id,
                        name=name,
                        value=value,
                        id_mean=mean,
                        id_std_dev=std_dev,
                        abnormal=is_abnormal,
                    )
                )
        return result

    def get_mean_labevents(self, hadm_ids: list[int]):
        result = []
        query = sq.get_mean_labevents_first_24h_icu
        db_result = self.execute_query(query, (hadm_ids,))
        for (
            itemid,
            subject_id,
            hadm_id,
            valuenum,
            valueuom,
            mean_value,
            std_dev,
            label,
            lower_ref,
            upper_ref,
        ) in db_result:
            is_abnormal = labevent_is_abnormal(
                valuenum=valuenum,
                lower_ref=lower_ref,
                upper_ref=upper_ref,
                mean=mean_value,
                std_dev=std_dev,
            )
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
                    label=label,
                    abnormal=is_abnormal,
                    upper_ref=upper_ref,
                    lower_ref=lower_ref,
                    valuenum=valuenum,
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
        query = sq.get_inputevents_first_24h_icu
        db_result = self.execute_query(query, (hadm_ids,))
        for (
            subject_id,
            hadm_id,
            itemid,
            amount,
            amountuom,
            ordercategoryname,
            label
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
                    label=label,
                )
            )
        return result

    def get_prescriptions(self, hadm_ids: list[int]):
        result = []
        query = sq.get_prescriptions_first_24h_icu
        db_result = self.execute_query(query, (hadm_ids,))
        for subject_id, hadm_id, drug, pharmacy_id, gsn, value in db_result:
            if not value:
                print("No value for prescription", drug, pharmacy_id, gsn, value)
            if value:
                result.append(
                    Prescription(
                        subject_id=subject_id,
                        hadm_id=hadm_id,
                        drug=drug,
                        pharmacy_id=pharmacy_id,
                        gsn=gsn,
                        value=value,
                    )
                )
        return result

    # Get from similarity tables
    def get_all_similarity_values(self):
        query = sq.all_scaled_similarity_values
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
    def get_endpoints_for_hadm_ids(self, hadm_ids: list[int]) -> tuple[int, int]:
        result = []
        (
            los_icu_result,
            los_hospital_result,
            icu_mortality,
            hosp_mortality,
            thirty_day_mortality,
            one_year_mortality,
        ) = (None, None, None, None, None, None)

        query = sq.endpoints_for_hadm_ids
        db_result = self.execute_query(  # [(los_icu, los_hospital), ...]
            query,
            (hadm_ids,),
        )

        for (
            hadm_id,
            los_icu,
            los_hospital,
            icu_mortality,
            hosp_mortality,
            thirty_day_mortality,
            one_year_mortality,
        ) in db_result:
            if los_hospital is not None:
                los_hospital_result = round(float(los_hospital), 1)
            if los_icu is not None:
                los_icu_result = round(float(los_icu), 1)
            result.append(
                {
                    "hadm_id": hadm_id,
                    "los_icu": los_icu_result,
                    "los_hospital": los_hospital_result,
                    "icu_mortality": icu_mortality,
                    "hospital_mortality": hosp_mortality,
                    "thirty_day_mortality": thirty_day_mortality,
                    "one_year_mortality": one_year_mortality,
                }
            )
        return result

    def get_labevent_by_id_for_hadm_ids(self, hadm_ids: list[int], item_id: int):
        query = sq.labevent_by_id_for_hadm_ids
        db_result = self.execute_query(query, (hadm_ids, item_id))
        result = []
        for (
            itemid,
            subject_id,
            hadm_id,
            valuenum,
            valueuom,
            id_mean,
            id_std_dev,
        ) in db_result:
            result.append(
                LabEvent(
                    value=itemid,
                    id=itemid,
                    itemid=itemid,
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    valueuom=valueuom,
                    valuenum=valuenum,
                    id_mean=id_mean,
                    id_std_dev=id_std_dev,
                )
            )
        return result

    def get_labevent_label(self, id: int):
        query = sq.labevent_label
        db_result = self.execute_query(query, (id,))
        return db_result[0][0]

    def def_get_inputevent_labels(self, id: list[int]):
        query = sq.inputevent_labels
        db_result = self.execute_query(query, (id,))
        return db_result
