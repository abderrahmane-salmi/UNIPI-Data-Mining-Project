import scipy.stats as stats
import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt

class DataQualityReporter():
    """
    Checks for missing and invalid values in a dataset.
    """
    def __init__(self, df: pd.DataFrame, feature_validator_functions: dict | None=None):
        self.df = df
        self.feature_validators = feature_validator_functions
        self.report = {}

    def __getitem__(self, report_key):
        """Returns the corresponding report value"""
        if report_key not in self.report.keys():
            return None
        else:
            return self.report[report_key]
    
    def compute_report(self):
        # Adds features with missing values to report
        missing = self.df.isna().sum()
        self.report['missing_values'] = missing[missing > 0]

        # Duplicate rows
        self.report['duplicate_rows'] = self.df[self.df.duplicated()]

        features = self.df.columns.to_list()
        self.report['not_validated'] = []
        self.report["invalid"] = {}

        # Checks for invalid values in categorical features
        for feature in features:
            if feature not in self.features_validators.keys():
                # if the dictionary doesn't contain a function for the feature then it is not validated
                self.report['not_validated'].append(feature)
            else:
                invalid_values_function = self.features_validators[feature]
                invalid = invalid_values_function(self.df, feature)
                if invalid != None:
                    self.report["invalid"][feature] = invalid
        return self.report
    
    def report_duplicated_rows(self):
        print(self.df["duplicated_rows"])


    def plot_missing_values(self):
        if self.report == {}:
            raise ValueError("Report is not computed")
        plt.figure(figsize=(14,6))
        sns.heatmap(self.report["missing_values"], cbar=False, cmap='viridis')
        plt.title("Missing values in all_tracks.csv")
        plt.show()


   

    
