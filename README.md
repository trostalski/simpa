# simpa

## Roadmap

1. Patient Similarity Framework
2. Select Cohort
3. Cluster Patients
4. Subcluster Analysis
5. FHIR translation

### Patient Similarity Framework

The Similarity of two patients is composed of thre different similarities categories. Namely, demographic similarity, disease similarity and laboratory similarity.
MIMIC-IV tables from the hosp-module that contribute to the different categories are:

- Demographic: patients, admissions
- Conditions: diagnoses_icd
- Observations: omr, labitems, labevents
- Treatment 

For each (sub-)catgory a similarity score is calculated as follows:
- age
- gender
- conditions
- observations

### Select Cohort

1. Sepsis 3 patients
This Cohort contains all patients that suffered from sepsis3 in the icu.
- 32.970 patients

### Cluster Patients
