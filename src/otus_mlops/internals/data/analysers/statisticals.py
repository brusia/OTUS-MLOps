
from abc import ABC
from pathlib import Path
from typing import Final, Union
import pandas as pd

from otus_mlops.internals.interfaces import REPORTS_PATH, IDataAnalyser
from logging import Logger, FileHandler
from scipy.stats import shapiro, mannwhitneyu, levene, ttest_ind

import logging

_logger: Logger = logging.getLogger(__name__)


REPORT_NAME: Final[str] = "statistical_tests.log"


# TODO: unify internal functions
class StatisticalTestsDataAnalyser(IDataAnalyser[pd.DataFrame, None]):
    def __init__(self, report_dir: str = ""):
        _logger.addHandler(FileHandler(REPORTS_PATH.joinpath(REPORT_NAME)))
        self._output_path = Path(report_dir) if report_dir else REPORTS_PATH

    def analyse(self, test_data: pd.DataFrame, ref_data: pd.DataFrame, feature_name: str) -> None:
        _logger.info("Analyse feature '%s'", feature_name)

        _logger.info("Run Shapiro-test for test_data")
        test_is_normal: bool = self._shapiro_test(test_data)

        _logger.info("Run Shapiro-test for ref_data")
        ref_is_normal: bool = self._shapiro_test(ref_data)

        if test_is_normal != ref_is_normal:
            _logger.warning("Test data and reference data distributions has different nature. Seems like splitting was inconsistent.")
        elif test_is_normal:
            _logger.info("Run Levene-test")
            if not self._levene_test(test_data, ref_data):
                _logger.warning("Using t-Student test may not be correct.")

            _logger.info("Run t-Student-test")
            self._t_student_test(test_data, ref_data)
        else:
            _logger.info("Run Mann-Whitneyu-test")
            self._mann_whitneyu_test(test_data, ref_data)

    def _shapiro_test(self, data_frame: pd.DataFrame) -> bool:
        res = shapiro(data_frame.values)
        _logger.info("Shapiro stat is %s, p-value = %s", res.statistic, res.pvalue)

        if res.pvalue >= 0.05:
            _logger.info("p-value = %s > 0.05, distribution is normal")
            return True
        else:
            _logger.warning("p-value = %s < 0.05, distribution is NOT normal")
            return False

    def _t_student_test(self, test_data: pd.DataFrame, ref_data: pd.DataFrame) -> bool:
        res = ttest_ind(test_data, ref_data)

        _logger.info("t-Student stat is %s, p-value = %s", res.statistic, res.pvalue)

        if res.pvalue >= 0.05:
            _logger.info("p-value = %s > 0.05, distribution are simular")
            return True
        else:
            _logger.warning("p-value = %s < 0.05, distribution are DIFFERENT")
            return False

    def _levene_test(self, test_data: pd.DataFrame, ref_data: pd.DataFrame,) -> bool:
        res = levene(test_data, ref_data)

        _logger.info("Levene stat is %s, p-value = %s", res.statistic, res.pvalue)

        if res.pvalue >= 0.05:
            _logger.info("p-value = %s > 0.05, variance are simular")
            return True
        else:
            _logger.warning("p-value = %s < 0.05, variance are DIFFERENT")
            return False
        

    def _mann_whitneyu_test(self, test_data: pd.DataFrame, ref_data: pd.DataFrame,) -> bool:
        res = mannwhitneyu(test_data.values, ref_data.values)
        _logger.info("Mann-Whitneyu stat is %s, p-value = %s", res.statistic, res.pvalue)

        if res.pvalue >= 0.05:
            _logger.info("p-value = %s > 0.05, distributions are the same")
            return True
        else:
            _logger.warning("p-value = %s < 0.05, distribution are DIFFERENT")
            return False

    def _kolmokorov_smirnov_test(self, test_data: pd.DataFrame, ref_data: pd.DataFrame,) -> bool:
        raise NotImplementedError

    def _population_stability_test(self, test_data: pd.DataFrame, ref_data: pd.DataFrame,) -> bool:
        raise NotImplementedError

