import sys
import os

# Go one level up from test/ to data_quality/
# so that 'checks' package becomes importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
