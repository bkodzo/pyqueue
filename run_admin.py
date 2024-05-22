"""Admin CLI launcher script."""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyqueue.admin import main

if __name__ == "__main__":
    main()

