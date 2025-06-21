import sys
print("Python executable:", sys.executable)
print("Virtual env active:", hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix))

# Тест импортов
try:
    import pandas as pd
    import selenium
    print("Packages installed successfully in virtual environment!")
except ImportError as e:
    print("Package import failed:", e)