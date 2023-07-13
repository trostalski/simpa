import os
import asyncio
from dotenv import load_dotenv

import asyncpg

load_dotenv()

TABlE_NAME = "gsn_ids"


async def main():
    conn = await asyncpg.connect(
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    await conn.execute(f"DROP TABLE IF EXISTS {TABlE_NAME}")
    await conn.execute(
        f"""
        CREATE TABLE {TABlE_NAME} (
            gsn VARCHAR(255),
            drug VARCHAR(255),
            id INT,
            PRIMARY KEY (gsn, drug)
        )
        """
    )
    prescription_records = await conn.fetch(
        """
        SELECT DISTINCT drug, gsn
        FROM mimiciv_hosp.prescriptions
        ORDER BY drug ASC
        """
    )
    id_counter = 0
    drug_map = {}
    gsn_map = {}
    for prescription in prescription_records:
        drug = prescription["drug"].strip()
        gsn = prescription["gsn"]
        gsns = gsn.split(" ") if gsn else []
        if drug not in drug_map:
            id_counter += 1
            drug_map[drug] = id_counter
            for gsn in gsns:
                if gsn not in gsn_map:
                    gsn_map[gsn] = id_counter
                for gsn in gsns:
                    if gsn not in gsn_map:
                        gsn_map[gsn] = id_counter
        else:
            id = drug_map[drug]
            if gsn not in gsn_map:
                gsn_map[gsn] = id_counter
            for gsn in gsns:
                if gsn not in gsn_map:
                    gsn_map[gsn] = id
    insert_list = []
    for gsn, id in gsn_map.items():
        drug_name = [drug for drug, drug_id in drug_map.items() if drug_id == id][0]
        gsn = gsn if gsn is not None else ""
        tupl = (gsn, drug_name, id)
        insert_list.append(tupl)
    await conn.executemany(
        f"""
        INSERT INTO {TABlE_NAME} (gsn, drug, id)
        VALUES ($1, $2, $3)
        """,
        insert_list,
    )
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
