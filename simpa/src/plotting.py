import pandas as pd
import itertools
import seaborn as sns
import matplotlib.pyplot as plt


def create_encounter_table(encounters):
    encounters = [e.dict() for e in encounters]
    # Create an empty dataframe
    df_list = []

    # Loop through each encounter and add a row to the dataframe
    for enc in encounters:
        # Get the relevant information from the encounter dictionary
        hadm_id = enc["hadm_id"]
        age = enc["demographics"]["age"]
        gender = enc["demographics"]["gender"]
        ethnicity = enc["demographics"]["ethnicity"]
        diagnoses = ", ".join([d["code"] for d in enc["diagnoses"]])
        labevents = ", ".join(
            [f"{l['item_id']}: {l['value']}" for l in enc["labevents"]]
        )
        vitalsigns = ", ".join([f"{v['id']}: {v['value']}" for v in enc["vitalsigns"]])
        inputevents = ", ".join([str(i["item_id"]) for i in enc["inputevents"]])

        # Add a row to the dataframe
        df_list.append(
            pd.DataFrame(
                {
                    "Age": age,
                    "Gender": gender,
                    "Ethnicity": ethnicity,
                    "Diagnoses": diagnoses,
                    "Labevents": labevents,
                    "Vitalsigns": vitalsigns,
                    "Inputevents": inputevents,
                },
                index=[hadm_id],
            )
        )

    # Concatenate the dataframes into a single dataframe
    df = pd.concat(df_list)

    # Swap the column and row names
    df = df.transpose()

    return df


def display_encounter_similarities(encounters):
    encounters = [e.dict() for e in encounters]
    for i, j in itertools.combinations(range(len(encounters)), 2):
        enc_i = encounters[i]
        enc_j = encounters[j]
        print(
            f"Comparing encounter {enc_i['hadm_id']} with encounter {enc_j['hadm_id']}:"
        )

        # Age difference
        age_i = enc_i["demographics"]["age"]
        age_j = enc_j["demographics"]["age"]
        print(f"Age difference: {abs(age_i - age_j)} years")

        # Gender comparison
        gender_i = enc_i["demographics"]["gender"]
        gender_j = enc_j["demographics"]["gender"]
        if gender_i == gender_j:
            print(f"Gender: {gender_i}")
        else:
            print(f"Gender difference")

        # Ethnicity comparison
        ethnicity_i = enc_i["demographics"]["ethnicity"]
        ethnicity_j = enc_j["demographics"]["ethnicity"]
        if ethnicity_i == ethnicity_j:
            print(f"Ethnicity: {ethnicity_i}")
        else:
            print("Ethnicity difference")

        # Common diagnoses
        diag_i = set([diag["code"] for diag in enc_i["diagnoses"]])
        diag_j = set([diag["code"] for diag in enc_j["diagnoses"]])
        common_diagnoses = diag_i.intersection(diag_j)
        if common_diagnoses:
            print(f"Common diagnoses codes: {', '.join(common_diagnoses)}")
        else:
            print("No common diagnoses codes")

        # Common labevents
        lab_i = {(lab["item_id"], lab["value"]) for lab in enc_i["labevents"]}
        lab_j = {(lab["item_id"], lab["value"]) for lab in enc_j["labevents"]}
        common_labevents = lab_i.intersection(lab_j)
        if common_labevents:
            print("Common labevent item ids:")
            for item_id, value in common_labevents:
                val_i = next(
                    lab["value"]
                    for lab in enc_i["labevents"]
                    if lab["item_id"] == item_id
                )
                val_j = next(
                    lab["value"]
                    for lab in enc_j["labevents"]
                    if lab["item_id"] == item_id
                )
                print(f"Item {item_id}: {abs(val_i - val_j)}")
        else:
            print("No common labevent item ids")

        # Common inputevents
        input_i = {inp["item_id"] for inp in enc_i["inputevents"]}
        input_j = {inp["item_id"] for inp in enc_j["inputevents"]}
        common_input = input_i.intersection(input_j)
        if common_input:
            print(
                f"Common inputevent item ids: {', '.join(map(str, list(common_input)))}"
            )
        else:
            print("No common inputevent item ids")

        # Vitalsigns difference
        vitals_i = {vital["id"]: vital["value"] for vital in enc_i["vitalsigns"]}
        vitals_j = {vital["id"]: vital["value"] for vital in enc_j["vitalsigns"]}
        common_vitals = set(vitals_i.keys()).intersection(set(vitals_j.keys()))
        if common_vitals:
            print("Common vitalsigns:")
            for vital_name in sorted(common_vitals):
                val_i = vitals_i[vital_name]
                val_j = vitals_j[vital_name]
                print(f"{vital_name}: {abs(val_i - val_j)}")
        else:
            print("No common vitalsigns")

        print()  # Blank line to separate encounters


# plot the los_hosp and los_icu for each cluster in a different scatterplot
def plot_endpoint_for_cluster(
    participants, cluster, endpoint, x_axis="cluster", hue="cluster", ax=None
):
    y_axis = endpoint
    data = pd.DataFrame(
        {
            x_axis: cluster.labels_,
            y_axis: [getattr(e, endpoint) for e in participants],
            hue: cluster.labels_,
        }
    )
    if ax is None:
        sns.scatterplot(x=x_axis, y=y_axis, hue=hue, data=data, palette="Set1")
    else:
        sns.scatterplot(x=x_axis, y=y_axis, hue=hue, data=data, palette="Set1", ax=ax)


def plot_endpoints_for_clusters(participants, clusters):
    endpoints = ["los_hosp", "los_icu", "icu_mortality", "hosp_mortality", "thirty_day_mortality", "one_year_mortality"]

    # Create a figure with two subplots
    fig, axes = plt.subplots(
        nrows=len(clusters), ncols=len(endpoints), figsize=(16, 32)
    )

    for col, cluster in enumerate(clusters):
        for row, endpoint in enumerate(endpoints):
            plot_endpoint_for_cluster(
                participants,
                cluster,
                endpoint,
                x_axis="cluster",
                hue="cluster",
                ax=axes[col][row],
            )
            axes[col][row].set_title(cluster.__class__.__name__)
            axes[col][row].set_xlabel("Cluster")
            axes[col][row].set_ylabel(endpoint)
            axes[col][row].legend().remove()


def plot_heatmap(df):
    pass
