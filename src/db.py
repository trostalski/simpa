import psycopg2

from schemas import Demographics, ICDDiagnosis, LabEvent


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

    def get_patient_demographics(self, subject_ids: list[int]):
        result = []
        query = "SELECT subject_id, anchor_age, gender FROM mimiciv_hosp.patients WHERE subject_id = ANY(%s);"
        db_result = self.execute_query(query, (subject_ids,))
        for subject_id, age, gender in db_result:
            result.append(Demographics(subject_id=subject_id, age=age, gender=gender))
        return result

    def get_mean_std_for_itemid(self, itemid: int):
        query = """
            SELECT mean, std
            FROM labevent_statistics
            WHERE itemid = %s;
        """
        db_result = self.execute_query(query, (itemid,))
        mean, std = db_result[0][0], db_result[0][1]
        return mean, std

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

    def get_labevents(self, hadm_ids: list[int]):
        result = []
        query = """SELECT labevent_id, le.itemid, subject_id, hadm_id, specimen_id, charttime,
                value, valuenum, valueuom, ref_range_lower, ref_range_upper, flag,
                priority, comments, label, fluid, category 
                FROM mimiciv_hosp.labevents le, mimiciv_hosp.d_labitems li 
                WHERE le.itemid = li.itemid and le.hadm_id = ANY(%s);"""
        db_result = self.execute_query(query, (hadm_ids,))
        for (
            id,
            item_id,
            subject_id,
            hadm_id,
            speciment_id,
            charttime,
            value,
            valuenum,
            valueuom,
            ref_range_lower,
            ref_range_upper,
            flag,
            priority,
            comments,
            label,
            fluid,
            category,
        ) in db_result:
            result.append(
                LabEvent(
                    labevent_id=id,
                    item_id=item_id,
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    specimen_id=speciment_id,
                    charttime=charttime,
                    value=value,
                    valuenum=valuenum,
                    valueuom=valueuom,
                    ref_range_lower=ref_range_lower,
                    ref_range_upper=ref_range_upper,
                    flag=flag,
                    priority=priority,
                    comments=comments,
                    label=label,
                    fluid=fluid,
                    category=category,
                )
            )
        return result
