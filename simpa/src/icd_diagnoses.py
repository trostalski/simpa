import os
from typing import Optional, Union
import requests
from pydantic import BaseModel
from dotenv import load_dotenv
from nxontology import NXOntology
import networkx as nx

from simpa.src.base_comparators import BaseComparator
from simpa.src.schemas import ICDDiagnosis
from simpa.src.schemas import SimilarityNode

load_dotenv()


def load_icd10_graph(path: str = "simpa/src/graphs/icd10_nx.gpickle"):
    try:
        G_default = nx.read_gpickle(path)
    except Exception:
        G_default = nx.read_gpickle("../graphs/icd10_nx.gpickle")
    G = NXOntology(G_default)
    G.freeze()
    print("Loaded ICD10 graph")
    return G


class ICDComparator(BaseComparator):
    def __init__(self, G: Optional[NXOntology] = None):
        if not G:
            self.G = load_icd10_graph()

    def compare(
        self,
        diagnoses_a: Union[list[ICDDiagnosis], ICDDiagnosis],
        diagnoses_b: Union[list[ICDDiagnosis], ICDDiagnosis],
    ) -> float:
        if isinstance(diagnoses_a, list) or isinstance(diagnoses_b, list):
            return self._compare_set(diagnoses_a, diagnoses_b)
        else:
            return self._compare_pair(diagnoses_a, diagnoses_b)

    def _compare_pair(
        self,
        diagnoses_a: ICDDiagnosis,
        diagnoses_b: ICDDiagnosis,
        ic_metric: str = None,
    ) -> float:
        similarity = self.get_node_similariy(
            diagnoses_a, diagnoses_b, ic_metric=ic_metric
        )
        return similarity

    def _compare_set(
        self,
        diagnoses_set_a: list[ICDDiagnosis],
        diagnoses_set_b: list[ICDDiagnosis],
        ic_metric: str = "intrinsic_ic_sanchez",
        cs_metric: str = "lin",
    ) -> Union[float, bool]:
        similarity = self.get_node_set_similarity(
            diagnoses_set_a, diagnoses_set_b, ic_metric=ic_metric, cs_metric=cs_metric
        )
        return similarity

    def get_node_set_similarity(
        self,
        nodes_a: list[SimilarityNode],
        nodes_b: list[SimilarityNode],
        ic_metric: str,
        cs_metric: str,
        method="jia",
    ):
        if method == "jia":
            result = self.node_set_similarity_jia(
                diagnoses_a=nodes_a,
                diagnoses_b=nodes_b,
                ic_metric=ic_metric,
                cs_metric=cs_metric,
            )
        return result

    def get_node_similariy(
        self,
        diagnosis_a: ICDDiagnosis,
        diagnosis_b: ICDDiagnosis,
        ic_metric: str,
    ):
        similarity = self.G.similarity(
            node_0=diagnosis_a.icd_code,
            node_1=diagnosis_b.icd_code,
            ic_metric=ic_metric,
        )
        return similarity.results()

    def node_set_similarity_jia(
        self,
        diagnoses_a: list[ICDDiagnosis],
        diagnoses_b: list[ICDDiagnosis],
        cs_metric: str = "lin",
        ic_metric: str = "intrinsic_ic_sanchez",
    ):
        """Derived from https://bmcmedinformdecismak.biomedcentral.com/articles/10.1186/s12911-019-0807-y"""
        lhs = 0
        rhs = 0
        node_sim_ba = []
        node_sim_ab = []

        for diagnosis_a in diagnoses_a:
            code_a = diagnosis_a.icd_code
            node_sim_ab = []
            for diagnosis_b in diagnoses_b:
                code_b = diagnosis_b.icd_code
                similarity = self.G.similarity(code_a, code_b, ic_metric)
                similarity = getattr(similarity, cs_metric)
                similarity *= (diagnosis_a.tfidf_score + diagnosis_b.tfidf_score) / 2
                node_sim_ab.append(similarity)
            lhs += max(node_sim_ab, default=0)

        for diagnosis_b in diagnoses_b:
            code_b = diagnosis_b.icd_code
            node_sim_ba = []
            for diagnosis_a in diagnoses_a:
                code_a = diagnosis_a.icd_code
                similarity = self.G.similarity(code_b, code_a, ic_metric)
                similarity = getattr(similarity, cs_metric)
                similarity *= (diagnosis_a.tfidf_score + diagnosis_b.tfidf_score) / 2
                node_sim_ba.append(similarity)
            rhs += max(node_sim_ba, default=0)

        if len(node_sim_ab) == 0 or len(node_sim_ba) == 0:
            return 0

        factor = 1 / (len(node_sim_ab) + len(node_sim_ba))
        result = factor * (lhs + rhs)
        return result
