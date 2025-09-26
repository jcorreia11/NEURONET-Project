import pandas as pd

class AllDataBuilder:
    def __init__(self, kepler_df: pd.DataFrame,
                 k8s_df: pd.DataFrame,
                 scaphander_vm_df: pd.DataFrame,
                 scaphander_host_df: pd.DataFrame,
                 pdu_df: pd.DataFrame,
                 proxmox_df: pd.DataFrame,
                 interval='1min'):
        self.kepler_df = kepler_df.copy()
        self.k8s_df = k8s_df.copy()
        self.scaphander_vm_df = scaphander_vm_df.copy()
        self.scaphander_host_df = scaphander_host_df.copy()
        self.pdu_df = pdu_df.copy()
        self.proxmox_df = proxmox_df.copy()
        self.interval = interval

    def preprocess_time(self):
        for df in [self.proxmox_df, self.scaphander_vm_df, self.scaphander_host_df,
                   self.pdu_df, self.k8s_df, self.kepler_df]:
            df['_time'] = pd.to_datetime(df['_time'])
            df['_time'] = df['_time'].dt.floor(self.interval)

    def aggregate_XXX(self):
        ...

    def join_data(self):
        ...

    def engineer_features(self):
        ...


    def build(self) -> pd.DataFrame:
        ...
        return self.dataset