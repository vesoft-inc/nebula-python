# Copyright 2025 vesoft-inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import sys

logger = logging.getLogger(__name__)

# Configure logging based on environment variables
log_level = os.getenv("NG_PYTHON_LOG_LEVEL", "INFO")
logger_sink = os.getenv("NG_PYTHON_LOG_SINK", "stdout")
debug_flag = os.getenv("NG_PYTHON_DEBUG", "false").lower() == "true"

# Set base log level
logger.setLevel(log_level)

# Add debug handler if debug logging enabled
if log_level == "DEBUG":
    # Create handler based on sink config
    handler = (
        logging.StreamHandler(sys.stdout)
        if logger_sink == "stdout"
        else logging.FileHandler(logger_sink)
    )

    # Add formatter for debug logs
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
