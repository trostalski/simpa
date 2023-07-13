class ClinicalEpisode:
    def __init__(self, id: int):
        self.id = id
        self.features = []

    def register(self, feature):
        self.features.append(feature)
