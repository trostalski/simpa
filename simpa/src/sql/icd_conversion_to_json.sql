COPY (
    SELECT
        json_build_object('data', json_agg(row_to_json(m)))
    FROM
        icd9_to_icd10_map m)
    TO '/Users/tillrostalski/Git/simpa/src/sql/icd9_to_icd10_map.json';

