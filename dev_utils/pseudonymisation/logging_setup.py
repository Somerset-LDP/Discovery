import logging
import os
from datetime import datetime


def setup_test_logging(test_name_prefix="test_results"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    test_results_dir = os.path.join(os.path.dirname(__file__), "test_results")
    os.makedirs(test_results_dir, exist_ok=True)
    filename = os.path.join(test_results_dir, f"{test_name_prefix}_{timestamp}.log")

    logging.getLogger().handlers.clear()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(filename),
            logging.StreamHandler()
        ]
    )
