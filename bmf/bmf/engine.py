"""
This is the Engine for creating transformations
"""
from typing import Dict
from tqdm import tqdm
from collections import OrderedDict
from .storage import Storage
from .functions import transformation_functions

class TransformationEngine\
            ():

    def __init__(self, config: Dict):
        """Initialize with config

        Parameters  
        ----------
        config : Dict
            The YAML configuration file as dict
        """

        self.input_path = config['input_file']
        self.output_path = config['output_file']
        self.transformations = config['transformations']

    def run(self):
        """
        Transform the input based on the
        transformation rules from the config
        """

        df = Storage.read(self.input_path, engine="openpyxl")
        #df = Storage.read(self.input_path)

        for transformation in tqdm(self.transformations):

            transformation_function = transformation_functions[transformation['action']]
            transformation.pop('action')

            df = df.apply(
                transformation_function,
                axis=1, 
                **transformation
            )

        # We only keep the target columns in the output
        target_columns = [transformation.get('target')
                          for transformation in self.transformations
                          if transformation.get('target') is not None]
        unique_target_columns = list(OrderedDict.fromkeys(target_columns))

        Storage.write(df[unique_target_columns], self.output_path)
