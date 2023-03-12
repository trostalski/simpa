import psycopg2

from schemas import Demographics, ICDDiagnosis, LabEvent, Vitalsign, InputEvent


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

    # Get categories
    def get_patient_demographics(self, subject_ids: list[int]):
        """Get age, gender and race for a list of subject_ids."""
        result = []
        query = """SELECT p.subject_id, p.anchor_age, p.gender, a.race
        FROM mimiciv_hosp.patients p, mimiciv_hosp.admissions a
        WHERE p.subject_id = ANY(%s) AND a.subject_id = p.subject_id;"""
        db_result = self.execute_query(query, (subject_ids,))
        for subject_id, age, gender, ethnicity in db_result:
            result.append(
                Demographics(
                    subject_id=subject_id, age=age, gender=gender, ethnicity=ethnicity
                )
            )
        return result

    def get_mean_vitalsigns(self, hadm_ids: list[int]):
        """Get mean vital signs for a list of hadm_ids."""
        result = []
        vital_sign_names = [
            "heart_rate",
            "sbp_ni",
            "dbp_ni",
            "mbp_ni",
            "resp_rate",
            "temperature",
            "spo2",
            "glucose",
        ]
        query = """
            SELECT i.subject_id, i.hadm_id, AVG(v.heart_rate), AVG(v.sbp_ni), AVG(v.dbp_ni), 
            AVG(v.mbp_ni), AVG(v.resp_rate), AVG(v.temperature), AVG(v.spo2), AVG(v.glucose)
            FROM mimiciv_derived.vitalsign v, mimiciv_icu.icustays i
            WHERE hadm_id = ANY(%s) AND v.stay_id = i.stay_id
            GROUP BY i.hadm_id, i.subject_id;
        """
        db_result = self.execute_query(query, (hadm_ids,))
        for values in db_result:
            subject_id = values[0]
            hadm_id = values[1]
            for name, value in zip(vital_sign_names, values[2:]):
                result.append(
                    Vitalsign(
                        id=name,
                        subject_id=subject_id,
                        hadm_id=hadm_id,
                        name=name,
                        value=value,
                    )
                )
        return result

    def get_mean_labevents(self, hadm_ids: list[int]):
        result = []
        query = """SELECT itemid, subject_id, hadm_id, AVG(valuenum), valueuom
                FROM mimiciv_hosp.labevents
                WHERE hadm_id = ANY(%s)
                GROUP BY itemid, hadm_id, subject_id, valueuom;
                """
        db_result = self.execute_query(query, (hadm_ids,))
        for (
            item_id,
            subject_id,
            hadm_id,
            valuenum,
            valueuom,
        ) in db_result:
            result.append(
                LabEvent(
                    id=item_id,
                    item_id=item_id,
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    value=valuenum,
                    valueuom=valueuom,
                )
            )
        return result

    def get_icd_diagnoses(self, hadm_ids: list[int]):
        result = []
        query = "SELECT * FROM mimiciv_hosp.diagnoses_icd WHERE hadm_id = ANY(%s);"
        db_result = self.execute_query(query, (hadm_ids,))
        for subject_id, hadm_id, seq_num, icd_code, icd_version in db_result:
            icd_code = icd_code.strip()  # icd_codes have trailing whitespace
            result.append(
                ICDDiagnosis(
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    seq_num=seq_num,
                    code=icd_code,
                    version=icd_version,
                )
            )
        return result

    def get_inputevents(self, hadm_ids: list[int]):
        result = []
        query = """SELECT subject_id, hadm_id, itemid, amount, amountuom, ordercategoryname
                FROM mimiciv_icu.inputevents
                WHERE hadm_id = ANY(%s);"""
        db_result = self.execute_query(query, (hadm_ids,))
        for (
            subject_id,
            hadm_id,
            item_id,
            amount,
            amount_uom,
            order_category_name,
        ) in db_result:
            result.append(
                InputEvent(
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    item_id=item_id,
                    amount=amount,
                    amount_uom=amount_uom,
                    order_category_name=order_category_name,
                )
            )
        return result

    # Get statistics
    def get_labevent_mean_std_for_itemid(self, itemid: int):
        query = """
            SELECT mean_value, std_dev
            FROM labevent_statistics
            WHERE itemid = %s;
        """
        db_result = self.execute_query(query, (itemid,))
        mean, std = db_result[0][0], db_result[0][1]
        return mean, std

    def get_vitalsign_mean_std_for_name(self, vitalsign_name: str):
        query = """
            SELECT mean_value, std_dev
            FROM vitalsign_statistics
            WHERE vitalsign_name = %s;
        """
        db_result = self.execute_query(query, (vitalsign_name,))
        mean, std = db_result[0][0], db_result[0][1]
        return mean, std

    # Get endpoints
    def get_endpoints_for_hadm_id(self, hadm_id: int) -> tuple[int, int]:
        los_icu_result, los_hospital_result = None, None
        db_result = self.execute_query(  # [(los_icu, los_hospital), ...]
            """
            SELECT los_icu, los_hospital
            FROM mimiciv_derived.icustay_detail
            Where hadm_id = %s;
            """,
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
