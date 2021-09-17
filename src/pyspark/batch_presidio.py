from typing import List, Optional, Dict, Union, Iterator, Iterable
import collections
from dataclasses import dataclass

from presidio_analyzer import AnalyzerEngine, RecognizerResult, RecognizerRegistry
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities.engine.result import EngineResult


import sys
sys.path.append('/opt/app/custom_recognizers/user_recognizer')
import user_recognizer

sys.path.append('/opt/app/custom_recognizers/custom_card_recognizer')
import custom_card_recognizer
@dataclass
class DictAnalyzerResult:
    """Hold the analyzer results per value or list of values."""
    key: str
    value: Union[str, List[str]]
    recognizer_results: Union[List[RecognizerResult], List[List[RecognizerResult]]]


class BatchAnalyzerEngine(AnalyzerEngine):
    """
    Class inheriting from AnalyzerEngine and adds the funtionality to analyze lists or dictionaries.
    """
    def __init__(self):
        registry = RecognizerRegistry()
        registry.load_predefined_recognizers()

        # Add the recognizer to the existing list of recognizers
        registry.add_recognizer(user_recognizer.get_recognizer())
        registry.add_recognizer(custom_card_recognizer.get_recognizer())

        # Set up analyzer with our updated recognizer registry
        AnalyzerEngine.__init__(self, registry=registry)
    
    def analyze_list(self, list_of_texts: Iterable[str], **kwargs) -> List[List[RecognizerResult]]:
        """
        Analyze an iterable of strings
        
        :param list_of_texts: An iterable containing strings to be analyzed.
        :param kwargs: Additional parameters for the `AnalyzerEngine.analyze` method.
        """
        
        list_results = []
        for text in list_of_texts:
            results = self.analyze(text=text, **kwargs) if isinstance(text, str) else []
            list_results.append(results)
        return list_results

    def analyze_dict(
     self, input_dict: Dict[str, Union[object, Iterable[object]]], **kwargs) -> Iterator[DictAnalyzerResult]:
        """
        Analyze a dictionary of keys (strings) and values (either object or Iterable[object]). 
        Non-string values are returned as is.
        
        :param input_dict: The input dictionary for analysis
        :param kwargs: Additional keyword arguments for the `AnalyzerEngine.analyze` method
        """
        
        for key, value in input_dict.items():
            if not value:
                results = []
            else:
                if isinstance(value, str):
                    results: List[RecognizerResult] = self.analyze(text=value, **kwargs)
                elif isinstance(value, collections.Iterable):
                    results: List[List[RecognizerResult]] = self.analyze_list(
                                list_of_texts=value, 
                                **kwargs)
                else:
                    results = []
            yield DictAnalyzerResult(key=key, value=value, recognizer_results=results)

class BatchAnonymizerEngine(AnonymizerEngine):
    """
    Class inheriting from the AnonymizerEngine and adding additional functionality 
    for anonymizing lists or dictionaries.
    """
    
    def anonymize_list(
        self, 
        texts:List[str], 
        recognizer_results_list: List[List[RecognizerResult]], 
        **kwargs
    ) -> List[EngineResult]:
        """
        Anonymize a list of strings.
        
        :param texts: List containing the texts to be anonymized (original texts)
        :param recognizer_results_list: A list of lists of RecognizerResult, 
        the output of the AnalyzerEngine on each text in the list.
        :param kwargs: Additional kwargs for the `AnonymizerEngine.anonymize` method
        """
        return_list = []
        for text, recognizer_results in zip(texts, recognizer_results_list):
            if isinstance(text,str):
                res = self.anonymize(text=text,analyzer_results=recognizer_results,**kwargs)
                return_list.append(res.text)
            else:
                return_list.append(text)

        return return_list


    def anonymize_dict(self, analyzer_results: Iterator[DictAnalyzerResult],**kwargs) -> Dict[str, str]:

        """
        Anonymize values in a dictionary.
        
        :param analyzer_results: Iterator of `DictAnalyzerResult` 
        containing the output of the AnalyzerEngine.analyze_dict on the input text.
        :param kwargs: Additional kwargs for the `AnonymizerEngine.anonymize` method
        """
        
        return_dict = {}
        for result in analyzer_results:
            if isinstance(result.value, str):
                resp = self.anonymize(text=result.value, analyzer_results=result.recognizer_results, **kwargs)
                return_dict[result.key] = resp.text
            elif isinstance(result.value, collections.Iterable):
                anonymize_respones = self.anonymize_list(texts=result.value,
                                                         recognizer_results_list=result.recognizer_results, 
                                                         **kwargs)
                return_dict[result.key] = anonymize_respones 
            else:
                return_dict[result.key] = result.value

        return return_dict